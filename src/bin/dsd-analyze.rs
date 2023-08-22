use clap::Parser;
use dogstatsd_utils::analysis::analyze_msgs;
use dogstatsd_utils::dogstatsdreader::{BufDogStatsDReader, DogStatsDReader};
use dogstatsd_utils::dogstatsdreplay::DogStatsDReplay;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

/// Process DogStatsD Replay messages
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File containing dogstatsd replay data
    #[arg(short, long)]
    input: Option<String>,
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    let reader: Box<dyn DogStatsDReader> = if let Some(input_file) = args.input {
        let file_path = Path::new(&input_file);
        let mut file = File::open(file_path)?;

        match DogStatsDReplay::try_from(&mut file) {
            Ok(replay) => Box::new(replay),
            Err(e) => {
                println!("Not a replay file, using regular bufreader, e: {}", e);
                Box::new(BufDogStatsDReader::try_from(file_path).expect("Uh-oh."))
            }
        }
    } else {
        // TODO should be able to support replay byte stream from stdin too
        Box::new(BufDogStatsDReader::new(Box::new(BufReader::new(
            io::stdin(),
        ))))
    };

    let msg_stats = analyze_msgs(reader)?;

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
