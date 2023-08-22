use std::env;
use std::fs::File;
use std::io::Error;

use dogstatsd_utils::dogstatsdreplay::DogStatsDReplay;

use clap::Parser;

/// Process DogStatsD Replay messages
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File containing dogstatsd replay data
    #[arg(short, long)]
    input: String,

    /// Where output dogstatsd messages should go
    #[arg(short, long)]
    output: Option<String>,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    let mut file = File::open(args.input)?;

    let mut replay = DogStatsDReplay::try_from(&mut file)?;

    if let Some(outpath) = args.output {
        if outpath == "-" {
            replay.print_msgs();
        } else {
            replay.write_to(&outpath)?;
            println!("Done! Result is in {}", outpath);
        }
    }

    Ok(())
}
