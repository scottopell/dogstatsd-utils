
use clap::Parser;
use dogstatsd_utils::analysis::analyze_msgs;
use dogstatsd_utils::dogstatsdreader::DogStatsDReader;
use dogstatsd_utils::init_logging;

use std::fs::{self};
use thiserror::Error;
use std::io::{self};
use std::path::Path;

#[derive(Error, Debug)]
pub enum AnalyzeError {
    #[error("Could not read dogstatsd from provided source")]
    ReaderFailure(#[from] dogstatsd_utils::dogstatsdreader::DogStatsDReaderError),
    #[error("IO Error")]
    Io(#[from] io::Error),
    #[error("Serde Error")]
    Serde(#[from] serde_yaml::Error),
}

/// Analyze DogStatsD traffic messages
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File containing dogstatsd data
    input: Option<String>,

    /// Emit lading DSD config
    #[arg(long, short, default_value_t = false)]
    lading_config: bool,
}

fn main() -> Result<(), AnalyzeError> {
    init_logging();
    let args = Args::parse();

    let mut reader = if let Some(input_file) = args.input {
        let file_path = Path::new(&input_file);

        let file = fs::File::open(file_path)?;
        DogStatsDReader::new(file)
    } else {
        DogStatsDReader::new(io::stdin().lock())
    }?;

    let msg_stats = analyze_msgs(&mut reader)?;

    println!("Name Length:\n{}", msg_stats.name_length);
    println!("# values per msg:\n{}", msg_stats.num_values);
    println!("# tags per msg:\n{}", msg_stats.num_tags);
    println!("# unicode tags per msg:\n{}", msg_stats.num_unicode_tags);
    println!("Metric Kind Breakdown:");
    for (kind, (cnt, per_type)) in msg_stats.kind.iter() {
        if let Some(per_type) = per_type {
            println!("{} Total {}", kind, cnt);
            for (t, cnt) in per_type.iter() {
                println!("{}: {}", t, cnt);
            }
        } else {
            println!("{}: {}", kind, cnt);
        }
    }
    println!();
    println!("# of Unique Tags: {}", msg_stats.total_unique_tags);
    println!("# of Contexts: {}", msg_stats.num_contexts);

    if args.lading_config {
        let lading_config = msg_stats.to_lading_config();
        let str_lading_config = serde_yaml::to_string(&lading_config)?;
        println!("Lading Config:\n{}", str_lading_config);
    }

    Ok(())
}
