use bytes::Bytes;
use clap::Parser;
use dogstatsd_utils::analysis::analyze_msgs;
use dogstatsd_utils::dogstatsdreader::DogStatsDReader;

use std::collections::HashMap;
use std::fs::{self};
use std::io::Read;
use std::io::{self};
use std::path::Path;

/// Process DogStatsD Replay messages
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File containing dogstatsd replay data
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

    let total_messages = msg_stats.len();
    let mut distribution_of_values = HashMap::new();
    for ds in msg_stats {
        let entry = distribution_of_values.entry(ds.num_values).or_insert(1);
        *entry += 1;
    }

    println!("Total messages: {}", total_messages);
    println!("Distribution of count of values:");
    for (count, occurrences) in distribution_of_values.iter() {
        println!("  {} values: {} occurrences", count, occurrences);
    }

    Ok(())
}
