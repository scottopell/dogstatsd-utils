use std::num::NonZeroU32;

use byte_unit::Byte;
use lading_throttle::Throttle;
use rand::{rngs::SmallRng, SeedableRng};
use thiserror::Error;

use clap::Parser;
use lading_payload::dogstatsd::{self, KindWeights, MetricWeights, ValueConf};

/// Generate random dogstatsd messages and emit them to stdout line-by-line
/// if num-msgs is specified, exactly that count of messages will be emitted
/// and then the program will exit. Otherwise it will run forever at --rate.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Emit this finite amount of msgs
    #[arg(short, long)]
    num_msgs: Option<u16>,

    /// Emit at this rate forever
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
        let bytes_per_second = Byte::from_str(rate).unwrap().get_bytes() as u32;
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
    } else {
        println!("{}", dd.generate(&mut rng));
    }

    Ok(())
}
