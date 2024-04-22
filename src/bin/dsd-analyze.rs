use chrono::{NaiveDateTime, TimeZone, Utc};
use human_bytes::human_bytes;
use std::time::Duration;

use clap::Parser;
use dogstatsd_utils::analysis::analyze_msgs;
use dogstatsd_utils::dogstatsdreader::DogStatsDReader;
use dogstatsd_utils::init_logging;
use sketches_ddsketch::DDSketch;

use std::fs::{self};
use std::io::{self};
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AnalyzeError {
    #[error("Could not read dogstatsd from provided source")]
    ReaderFailure(#[from] dogstatsd_utils::dogstatsdreader::DogStatsDReaderError),
    #[error("IO Error")]
    Io(#[from] io::Error),
    #[error("Serde Error")]
    Serde(#[from] serde_yaml::Error),
    #[error("Serde Error json")]
    SerdeJSON(#[from] serde_json::Error),
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

    /// Show all unique tags with count
    #[arg(long, short, default_value_t = false)]
    print_unique_tags: bool,
}

/// Prints out a quick summary of a given sketch
/// Future improvement would be a visual histogram in the terminal
/// similar to what `histo` offered
fn sketch_to_string(sketch: &DDSketch) -> String {
    let (Some(min), Some(max), Some(sum), count) =
        (sketch.min(), sketch.max(), sketch.sum(), sketch.count())
    else {
        return "No data".to_string();
    };
    let mean = sum / count as f64;
    // should be safe to unwrap since we know we have data
    let twenty = sketch.quantile(0.2).unwrap().unwrap();
    let fourty = sketch.quantile(0.4).unwrap().unwrap();
    let sixty = sketch.quantile(0.6).unwrap().unwrap();
    let eighty = sketch.quantile(0.8).unwrap().unwrap();

    format!("\tmin: {}\n\t0.2: {:.1}\n\t0.4: {:.1}\n\t0.5: {:.1}\n\t0.6: {:.1}\n\t0.8: {:.1}\n\tmax: {}\n\tcount: {}", min, twenty, fourty, mean, sixty, eighty, max, count)
}

fn epoch_duration_to_datetime(epoch: Duration) -> chrono::DateTime<chrono::Utc> {
    let naive_datetime =
        NaiveDateTime::from_timestamp_nanos(epoch.as_nanos().try_into().unwrap()).unwrap();
    Utc.from_utc_datetime(&naive_datetime)
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
    if let Some(ref reader_analytics) = msg_stats.reader_analytics {
        println!("Reader Analytics:");
        let first_timestamp = epoch_duration_to_datetime(reader_analytics.earliest_timestamp);
        let last_timestamp = epoch_duration_to_datetime(reader_analytics.latest_timestamp);
        println!("\tTransport: {}", reader_analytics.transport_type);
        println!("\tFirst packet time: {}", first_timestamp.to_rfc3339());
        println!("\tLast packet time: {}", last_timestamp.to_rfc3339());
        println!("\tDuration: {:?}", reader_analytics.duration());
        println!("\tTotal Packets: {}", reader_analytics.total_packets);
        println!(
            "\tTotal Bytes: {}",
            human_bytes(reader_analytics.total_bytes as f64)
        );
        println!("\tTotal Messages: {}", reader_analytics.total_messages);

        println!(
            "\tAverage Bytes Per Second:  {} per second",
            human_bytes(reader_analytics.average_bytes_per_second())
        );

        println!(
            "\tMessage Length:\n{}",
            sketch_to_string(&reader_analytics.message_length)
        );
    }

    println!("Traffic Analytics:");
    println!("Name Length:\n{}", sketch_to_string(&msg_stats.name_length));
    println!(
        "Tag Length:\n{}",
        sketch_to_string(&msg_stats.tag_total_length)
    );
    println!(
        "# values per msg:\n{}",
        sketch_to_string(&msg_stats.num_values)
    );
    println!("# tags per msg:\n{}", sketch_to_string(&msg_stats.num_tags));
    println!(
        "# unicode tags per msg:\n{}",
        sketch_to_string(&msg_stats.num_unicode_tags)
    );
    println!("# of Unique Tags:\n\t{}", msg_stats.unique_tags.len());
    println!("# of Contexts:\n\t{}", msg_stats.num_contexts);
    println!(
        "Unique Tag / # Contexts ratio:\n\t{:.2}",
        (msg_stats.unique_tags.len() as f64) / (msg_stats.num_contexts as f64)
    );

    println!();
    println!("Message Kind Breakdown:");
    for (kind, (cnt, per_type)) in msg_stats.kind.iter() {
        println!("\t{}: {}", kind, cnt);
        if let Some(per_type) = per_type {
            for (t, cnt) in per_type.iter() {
                println!("\t\t{}: {}", t, cnt);
            }
        }
    }
    if args.print_unique_tags {
        println!("Unique tags:");
        let mut unique_tags: Vec<(&String, &u32)> = msg_stats.unique_tags.iter().collect();

        unique_tags.sort_by(|a, b| a.1.cmp(b.1));

        // Print sorted entries
        for (key, value) in unique_tags {
            println!("{}  {}", value, key);
        }
    }

    if args.lading_config {
        let str_lading_config = msg_stats
            .to_lading_config_str()
            .expect("Error converting to lading config");
        println!("Lading Config:\n---\n{}---", str_lading_config);
    }

    Ok(())
}
