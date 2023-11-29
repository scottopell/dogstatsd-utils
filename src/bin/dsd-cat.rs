use std::fs;
use std::fs::File;
use std::io::stdout;
use std::io::Error;
use std::io::Read;
use std::io::{self};
use std::path::Path;

use bytes::Bytes;
use dogstatsd_utils::analysis::print_msgs;
use dogstatsd_utils::dogstatsdreader::DogStatsDReader;

use clap::Parser;

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

fn main() -> Result<(), Error> {
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
