use std::fs;
use std::fs::File;
use std::io::stdout;

use std::io::{self};
use std::path::Path;
use thiserror::Error;

use dogstatsd_utils::analysis::print_msgs;
use dogstatsd_utils::dogstatsdreader::DogStatsDReader;

use clap::Parser;
use dogstatsd_utils::init_logging;

/// Take data from the specified input file and write it either to stdout or to a specified file.
/// Data can be raw utf-8 text or a dogstatsd-replay file, optionally zstd encoded.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File containing dogstatsd data
    input: Option<String>,

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

fn main() -> Result<(), CatError> {
    init_logging();
    let args = Args::parse();

    let mut reader = if let Some(input_file) = args.input {
        let file_path = Path::new(&input_file);

        let file = fs::File::open(file_path)?;
        DogStatsDReader::new(file)
    } else {
        DogStatsDReader::new(io::stdin().lock())
    }?;

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
