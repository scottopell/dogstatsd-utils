use bytes::Bytes;
use clap::Parser;
use dogstatsd_utils::analysis::analyze_msgs;
use dogstatsd_utils::dogstatsdreader::DogStatsDReader;

use std::fs::{self};
use std::io::Read;
use std::io::{self};
use std::path::Path;

/// Analyze DogStatsD traffic messages
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File containing dogstatsd data
    input: Option<String>,
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    let bytes: Bytes = if let Some(input_file) = args.input {
        let file_path = Path::new(&input_file);

        Bytes::from(fs::read(file_path).unwrap())
    } else {
        let mut contents = Vec::new();
        // TODO handle empty stream better probably
        io::stdin().read_to_end(&mut contents).unwrap();
        Bytes::from(contents)
    };

    let mut reader = DogStatsDReader::new(bytes);

    let msg_stats = analyze_msgs(&mut reader)?;

    println!("Name Length:\n{}", msg_stats.name_length);
    println!("# values per msg:\n{}", msg_stats.num_values);
    println!("# tags per msg:\n{}", msg_stats.num_tags);
    println!("# unicode tags per msg:\n{}", msg_stats.num_unicode_tags);
    println!("Metric Kind Breakdown:");
    for (kind, num_samples) in msg_stats.kind.iter() {
        println!("{}: {}", kind, num_samples);
    }
    println!();
    println!("# of Unique Tags: {}", msg_stats.total_unique_tags);

    Ok(())
}
