use std::fs;
use std::fs::File;
use std::io::stdout;

use std::io::{self};
use std::path::Path;
use thiserror::Error;
use anyhow::Result;

use dogstatsd_utils::analysis::print_msgs;
use dogstatsd_utils::dogstatsdreader::DogStatsDReader;
use dogstatsd_utils::udpbytebufreader::UdpByteBufReader;

use clap::Parser;
use dogstatsd_utils::init_logging;
use tracing::info;

/// Take data from the specified input file and write it either to stdout or to a specified file.
/// Data can be raw utf-8 text or a dogstatsd-replay file, optionally zstd encoded.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File containing dogstatsd data
    input: Option<String>,

    /// Species port to listen for UDP packets on
    #[arg(long)]
    input_udp_port: Option<String>,

    /// Where output dogstatsd messages should go
    #[arg(short, long)]
    output: Option<String>,
}

#[derive(Error, Debug)]
pub enum CatError {
    #[error("Could not read dogstatsd from provided source")]
    ReaderFailure(#[from] dogstatsd_utils::dogstatsdreader::DogStatsDReaderError),
    #[error("IO Error")]
    Io(#[from] io::Error),
}


fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();

    let mut reader = if let Some(input_file) = args.input {
        let file_path = Path::new(&input_file);

        let file = fs::File::open(file_path)?;
        DogStatsDReader::new(file)
    } else if let Some(input_udp_port) = args.input_udp_port {
        // https://github.com/vectordotdev/vector/blob/c12c8e1bef9fb2f9a9a31892d7911b8637f581e7/src/sources/statsd/mod.rs#L289-L301
        let udp_reader = UdpByteBufReader::new("127.0.0.1", &input_udp_port)?;
        DogStatsDReader::new(udp_reader)
    } else {
        DogStatsDReader::new(io::stdin().lock())
    }?;

    info!("Created reader");
    if let Some(outpath) = args.output {
        if outpath == "-" {
            print_msgs(&mut reader, stdout());
        } else {
            print_msgs(&mut reader, File::create(outpath)?);
        }
    } else {
        print_msgs(&mut reader, stdout());
    };

    Ok(())
}
