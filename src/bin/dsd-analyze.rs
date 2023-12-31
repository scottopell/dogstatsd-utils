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
        // and consolidate this amongst dsd-cat and dsd-analyze
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

    Ok(())
}
