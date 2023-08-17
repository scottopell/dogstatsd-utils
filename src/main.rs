use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

enum Kind {
    Count,
    Distribution,
    Gauge,
    Timer,
    Histogram,
    Set,
}

struct DogStatsDMessageStats {
    name_length: u16,
    num_values: u16,
    num_tags: u16,
    num_ascii_tags: u16,
    num_unicode_tags: u16,
    kind: Option<Kind>,
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut reader: Box<dyn BufRead> = if args.len() > 1 {
        let file_path = &args[1];
        let file = File::open(file_path)?;
        let file_size = file.metadata()?.len();
        let progress_bar = ProgressBar::new(file_size);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes} ({eta})")
                .progress_chars("#>-"),
        );

        Box::new(BufReader::with_capacity(1024, progress_bar.wrap_read(file)))
    } else {
        Box::new(BufReader::new(io::stdin()))
    };

    let mut msg_stats: Vec<DogStatsDMessageStats> = Vec::new();

    let mut line = String::new();
    while reader.read_line(&mut line)? > 0 {
        let parts: Vec<&str> = line.split('|').collect();
        let name_and_values: Vec<&str> = parts[0].split(':').collect();

        let name = name_and_values[0];

        let last_part = parts[parts.len() - 1];
        let mut num_tags = 0;
        let mut num_ascii_tags = 0;
        let mut num_unicode_tags = 0;
        if last_part.starts_with("#") {
            // these are tags
            let tags = last_part.split(',');
            for tag in tags {
                num_tags += 1;
                if tag.is_ascii() {
                    num_ascii_tags += 1;
                } else {
                    num_unicode_tags += 1;
                }
            }
        }

        let kind = match parts.get(1) {
            Some(s) => match *s {
                "d" => Some(Kind::Distribution),
                "ms" => Some(Kind::Timer),
                "g" => Some(Kind::Gauge),
                "c" => Some(Kind::Count),
                "s" => Some(Kind::Set),
                "h" => Some(Kind::Histogram),
                _ => {
                    println!("Found unknown msg type for dogstatsd msg: {}", line);
                    None

                }
            }
            _ => {
                println!("Found unusual dogstatsd msg: {}", line);
                None
            }
        };

        let ds = DogStatsDMessageStats {
            name_length: name.len() as u16,
            num_values: name_and_values.len() as u16 - 1,
            num_tags,
            num_ascii_tags,
            num_unicode_tags,
            kind,
        };

        msg_stats.push(ds);
        line.clear();
    }

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
