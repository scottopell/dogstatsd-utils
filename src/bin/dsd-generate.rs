use std::{num::NonZeroU32, time::Duration};

use dogstatsd_utils::rate::{parse_rate, RateSpecification};
use lading_throttle::Throttle;
use rand::{rngs::SmallRng, SeedableRng};
use thiserror::Error;

use clap::Parser;
use lading_payload::dogstatsd::{self, KindWeights, MetricWeights, ValueConf};
use tokio::time::sleep;

/// Generate random dogstatsd messages and emit them to stdout line-by-line.
/// If no options are specified, then it will emit a single message and exit.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Emit this finite amount of msgs
    #[arg(short, long)]
    num_msgs: Option<u16>,

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
    let args = Args::parse();

    if args.num_msgs.is_some() && args.rate.is_some() {
        return Err(DSDGenerateError::InvalidArgs);
    }

    let mut rng = SmallRng::seed_from_u64(34512423);
    let dd = dogstatsd::DogStatsD::new(
        100..500,
        5..10,
        5..10,
        5..10,
        0..10,
        1..10,
        0.08,
        KindWeights::default(),
        MetricWeights::default(),
        ValueConf::default(),
        &mut rng,
    );
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
