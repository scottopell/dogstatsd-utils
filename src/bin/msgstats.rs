use dogstatsd_utils::dogstatsdreader::DogStatsDReader;
use dogstatsd_utils::dogstatsdreplay::DogStatsDReplay;
use dogstatsd_utils::msgstats::analyze_msgs;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

struct BufDogStatsDReader {
    reader: Box<dyn BufRead>,
}

impl TryFrom<&Path> for BufDogStatsDReader {
    type Error = io::Error;

    fn try_from(p: &Path) -> Result<Self, Self::Error> {
        // Q: why do I not need to declare this file as mutable
        // and give a mutable reference to BufReader::new?
        // Is it because I'm transfering ownership?
        //
        // Related:
        // Why can I not do the same thing for
        //  DogStatsDReplay::TryFrom<File>
        // ?  I currently have
        //  DogStatsDReplay::TryFrom<&mut File>
        // but I don't like this. I read out some bytes from the file in TryFrom
        // so the file is in a unknown state after.
        // I'd rather transfer ownership to TryFrom, but I get an error saying
        // "File must be mutable"
        // Not sure what I'm missing here.

        let file = File::open(p)?;

        Ok(BufDogStatsDReader {
            reader: Box::new(BufReader::new(file)),
        })
    }
}

impl DogStatsDReader for BufDogStatsDReader {
    fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize> {
        match self.reader.read_line(s) {
            Ok(n) => {
                return if n == 0 {
                    // EOF
                    Ok(0)
                } else {
                    Ok(1)
                };
            }
            Err(e) => Err(e),
        }
    }
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut reader: Box<dyn DogStatsDReader> = if args.len() > 1 {
        let file_path = Path::new(&args[1]);
        let mut file = File::open(file_path)?;

        match DogStatsDReplay::try_from(&mut file) {
            Ok(replay) => Box::new(replay),
            Err(e) => {
                println!("Not a replay file, using regular bufreader, e: {}", e);
                Box::new(BufDogStatsDReader::try_from(file_path).expect("Uh-oh."))
            }
        }
    } else {
        Box::new(BufDogStatsDReader {
            reader: Box::new(BufReader::new(io::stdin())),
        })
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
