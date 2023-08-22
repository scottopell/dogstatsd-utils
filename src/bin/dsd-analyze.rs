use bytes::Bytes;
use clap::Parser;
use dogstatsd_utils::analysis::analyze_msgs;
use dogstatsd_utils::dogstatsdreader::DogStatsDReader;
use histo::Histogram;

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

    println!("Total messages: {}", msg_stats.len());

    let mut num_value_histogram = Histogram::with_buckets(10);
    let mut name_length_histogram = Histogram::with_buckets(10);
    let mut num_tags_histogram = Histogram::with_buckets(10);
    let mut num_ascii_tags_histogram = Histogram::with_buckets(10);
    let mut num_unicode_tags_histogram = Histogram::with_buckets(10);

    for ds in msg_stats {
        num_value_histogram.add(ds.num_values as u64);
        name_length_histogram.add(ds.name_length as u64);
        num_tags_histogram.add(ds.num_tags as u64);
        num_ascii_tags_histogram.add(ds.num_ascii_tags as u64);
        num_unicode_tags_histogram.add(ds.num_unicode_tags as u64);
    }

    println!("# values per msg: {}", num_value_histogram);
    println!("Name length: {}", name_length_histogram);
    println!("# of Tags: {}", num_tags_histogram);
    println!("# of ascii Tags: {}", num_ascii_tags_histogram);
    println!("# of unicode Tags: {}", num_unicode_tags_histogram);

    Ok(())
}
