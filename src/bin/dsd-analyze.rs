use chrono::{NaiveDateTime, TimeZone, Utc};
use std::time::Duration;
use human_bytes::human_bytes;

use sketches_ddsketch::DDSketch;
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

/// Prints out a quick summary of a given sketch
/// Future improvement would be a visual histogram in the terminal
/// similar to what `histo` offered
fn sketch_to_string(sketch: &DDSketch) -> String {
    let (Some(min), Some(max), Some(sum), count) = (sketch.min(), sketch.max(), sketch.sum(), sketch.count()) else {
        return "No data".to_string();
    };
    format!("min: {}, max: {}, mean: {}, count: {}", min, max, (sum / count as f64), count)
}

fn epoch_duration_to_datetime(epoch: Duration) -> chrono::DateTime<chrono::Utc> {
    let naive_datetime = NaiveDateTime::from_timestamp_nanos(epoch.as_nanos().try_into().unwrap()).unwrap();
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

    println!("Total Messages:\n {}", msg_stats.num_msgs);
    println!("Name Length:\n{}", sketch_to_string(&msg_stats.name_length));
    println!("# values per msg:\n{}", sketch_to_string(&msg_stats.num_values));
    println!("# tags per msg:\n{}", sketch_to_string(&msg_stats.num_tags));
    println!("# unicode tags per msg:\n{}", sketch_to_string(&msg_stats.num_unicode_tags));
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
        let lading_config = msg_stats.to_lading_config().expect("Error converting to lading config");
        let str_lading_config = serde_yaml::to_string(&lading_config)?;
        println!("Lading Config:\n{}", str_lading_config);
    }

    if let Some(reader_analytics) = msg_stats.reader_analytics {
        println!("Reader Analytics:");
        let first_timestamp = epoch_duration_to_datetime(reader_analytics.earliest_timestamp);
        let last_timestamp = epoch_duration_to_datetime(reader_analytics.latest_timestamp);
        println!("First packet time: {}", first_timestamp.to_rfc3339());
        println!("Last packet time: {}", last_timestamp.to_rfc3339());
        println!("Duration: {:?}", reader_analytics.latest_timestamp - reader_analytics.earliest_timestamp);
        println!("Total Packets: {}", reader_analytics.total_packets);
        println!("Total Bytes: {}", reader_analytics.total_bytes);
        println!("Total Messages: {}", reader_analytics.total_messages);

        let duration = reader_analytics.latest_timestamp - reader_analytics.earliest_timestamp;
        let avg_throughput = reader_analytics.total_bytes as f64 / duration.as_secs_f64();
        println!("Average Bytes Per Second:  {} per second", human_bytes(avg_throughput));
    }

    Ok(())
}
