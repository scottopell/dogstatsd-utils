use std::{num::NonZeroU32, time::Duration};

use dogstatsd_utils::rate::{parse_rate, RateSpecification};
use lading_throttle::Throttle;
use rand::{rngs::SmallRng, SeedableRng};
use thiserror::Error;

use clap::Parser;
use lading_payload::dogstatsd::{self, KindWeights, MetricWeights, ValueConf};
use tokio::time::sleep;
use tracing::info;

/// Generate random dogstatsd messages and emit them to stdout line-by-line.
/// If no options are specified, then it will emit a single message and exit.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Emit this finite amount of msgs
    #[arg(short, long)]
    num_msgs: Option<u16>,

    /// metric_types is optional and if specified will emit only metrics of the given types
    #[arg(long, value_delimiter = ',')]
    metric_types: Option<Vec<String>>,

    /// Rate can be specified as throughput (ie, bytes per second) or time (ie 1hz)
    /// eg '1kb' or '10 hz'
    #[arg(short, long)]
    rate: Option<String>,

    /// Where output dogstatsd messages should go
    #[arg(short, long)]
    output: Option<String>,
}

#[derive(Error, Debug)]
pub enum DSDGenerateError {
    #[error("Invalid arguments specified")]
    InvalidArgs,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), DSDGenerateError> {
    let info_or_env = tracing_subscriber::filter::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::fmt()
        .with_env_filter(info_or_env)
        .init();
    let args = Args::parse();

    if args.num_msgs.is_some() && args.rate.is_some() {
        return Err(DSDGenerateError::InvalidArgs);
    }

    let mut rng = SmallRng::seed_from_u64(34512423);
    let mut metric_weights = MetricWeights::default();
    if let Some(metric_types) = args.metric_types {
        let metric_str_types = metric_types
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<&str>>();
        info!("metric_str_types: {:?}", metric_str_types);
        let count_weight: u8 =
            if metric_str_types.contains(&"count") || metric_str_types.contains(&"c") {
                1
            } else {
                0
            };
        let gauge_weight: u8 =
            if metric_str_types.contains(&"gauge") || metric_str_types.contains(&"g") {
                1
            } else {
                0
            };
        let histogram_weight: u8 =
            if metric_str_types.contains(&"histogram") || metric_str_types.contains(&"h") {
                1
            } else {
                0
            };
        let set_weight: u8 = if metric_str_types.contains(&"set") || metric_str_types.contains(&"s")
        {
            1
        } else {
            0
        };
        let timing_weight: u8 =
            if metric_str_types.contains(&"timing") || metric_str_types.contains(&"t") {
                1
            } else {
                0
            };
        let distribution_weight: u8 = if metric_str_types.contains(&"distribution")
            || metric_str_types.contains(&"d")
            || metric_str_types.contains(&"sketch")
        {
            1
        } else {
            0
        };
        metric_weights = MetricWeights::new(
            count_weight,
            gauge_weight,
            timing_weight,
            distribution_weight,
            set_weight,
            histogram_weight,
        );
    }
    let dd = dogstatsd::DogStatsD::new(
        // Contexts
        dogstatsd::ConfRange::Inclusive { min: 100, max: 500 },
        // Service check name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_key_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_value_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tags_per_msg
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_count
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_pack_probability
        0.08,
        KindWeights::default(),
        metric_weights,
        ValueConf::default(),
        &mut rng,
    )
    .expect("Failed to create dogstatsd generator");

    if let Some(num_msgs) = args.num_msgs {
        for _ in 0..num_msgs {
            println!("{}", dd.generate(&mut rng));
        }
    } else if let Some(rate) = args.rate {
        match parse_rate(&rate) {
            Some(RateSpecification::TimerBased(hz_value)) => loop {
                let sleep_in_ms = 1000 / (hz_value as u64);
                sleep(Duration::from_millis(sleep_in_ms)).await;
                println!("{}", dd.generate(&mut rng));
            },
            Some(RateSpecification::ThroughputBased(bytes_per_second)) => {
                let mut throttle = Throttle::new_with_config(
                    lading_throttle::Config::default(),
                    NonZeroU32::new(bytes_per_second).unwrap(),
                );
                loop {
                    let msg = dd.generate(&mut rng);
                    let msg_str = msg.to_string();
                    let _ = throttle
                        .wait_for(NonZeroU32::new(msg_str.len() as u32).unwrap())
                        .await;
                    println!("{}", msg_str);
                }
            }
            None => {
                println!("Invalid rate specified, couldn't parse '{}'", rate);
            }
        }
    } else {
        println!("{}", dd.generate(&mut rng));
    }

    Ok(())
}
