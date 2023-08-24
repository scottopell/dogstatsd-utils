use rand::{rngs::SmallRng, SeedableRng};
use thiserror::Error;

use clap::Parser;
use lading_payload::{dogstatsd, Serialize};

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

fn main() -> Result<(), DSDGenerateError> {
    let args = Args::parse();

    if args.num_msgs.is_some() && args.rate.is_some() {
        return Err(DSDGenerateError::InvalidArgs);
    } else if let Some(num_msgs) = args.num_msgs {
        let mut rng = SmallRng::seed_from_u64(34512223);
        let dd = dogstatsd::DogStatsD::default(&mut rng);
        for _ in 0..num_msgs {
            println!("{}", dd.generate(&mut rng));
        }
    } else {
        todo!("Rate not implemented yet.");
    }

    Ok(())
}
