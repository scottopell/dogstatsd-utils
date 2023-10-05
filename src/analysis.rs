use std::{collections::HashMap, fmt::Display, io::Write};

use histo::Histogram;

use crate::dogstatsdreader::DogStatsDReader;

#[derive(Hash, PartialEq, Eq)]
pub enum Kind {
    Count,
    Distribution,
    Gauge,
    Timer,
    Histogram,
    Set,
    ServiceCheck,
    Event,
}

impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Kind::Count => write!(f, "Count"),
            Kind::Distribution => write!(f, "Distribution"),
            Kind::Gauge => write!(f, "Gauge"),
            Kind::Timer => write!(f, "Timer"),
            Kind::Histogram => write!(f, "Histogram"),
            Kind::Set => write!(f, "Set"),
            Kind::ServiceCheck => write!(f, "ServiceCheck"),
            Kind::Event => write!(f, "Event"),
        }
    }
}

pub struct DogStatsDBatchStats {
    pub name_length: Histogram,
    pub num_values: Histogram,
    pub num_tags: Histogram,
    pub num_unicode_tags: Histogram,
    pub kind: HashMap<Kind, u16>,
}

pub fn print_msgs<T>(reader: &mut DogStatsDReader, mut out: T)
where
    T: Write,
{
    let mut line = String::new();
    while let Ok(num_read) = reader.read_msg(&mut line) {
        if num_read == 0 {
            // EOF
            break;
        }
        out.write_all(line.as_bytes()).unwrap();
        out.write_all(b"\n").unwrap();
        line.clear();
    }
}

pub fn analyze_msgs(reader: &mut DogStatsDReader) -> Result<DogStatsDBatchStats, std::io::Error> {
    let default_num_buckets = 10;
    let mut msg_stats = DogStatsDBatchStats {
        name_length: Histogram::with_buckets(default_num_buckets),
        num_values: Histogram::with_buckets(default_num_buckets),
        num_tags: Histogram::with_buckets(default_num_buckets),
        num_unicode_tags: Histogram::with_buckets(default_num_buckets),
        kind: HashMap::new(),
    };
    // TODO add num_contexts to this, requires some more computation to
    // separate the tags and put it in a hashset probably

    msg_stats.kind.insert(Kind::Count, 0);
    msg_stats.kind.insert(Kind::Distribution, 0);
    msg_stats.kind.insert(Kind::Event, 0);
    msg_stats.kind.insert(Kind::Gauge, 0);
    msg_stats.kind.insert(Kind::Histogram, 0);
    msg_stats.kind.insert(Kind::ServiceCheck, 0);
    msg_stats.kind.insert(Kind::Set, 0);

    let mut line = String::new();
    while let Ok(num_read) = reader.read_msg(&mut line) {
        if num_read == 0 {
            // EOF
            break;
        }
        let parts: Vec<&str> = line.split('|').collect();
        let name_and_values: Vec<&str> = parts[0].split(':').collect();
        let num_values = name_and_values.len() as u64 - 1;

        let name = name_and_values[0];

        let last_part = parts[parts.len() - 1];
        let mut num_tags = 0;
        let mut num_unicode_tags = 0;
        if last_part.starts_with('#') {
            // these are tags
            let tags = last_part.split(',');
            for tag in tags {
                num_tags += 1;
                if !tag.is_ascii() {
                    num_unicode_tags += 1;
                }
            }
        }

        msg_stats.name_length.add(name.len() as u64);
        msg_stats.num_tags.add(num_tags);
        msg_stats.num_unicode_tags.add(num_unicode_tags);
        msg_stats.num_values.add(num_values);

        match parts.get(1) {
            Some(s) => match *s {
                "d" => {
                    msg_stats
                        .kind
                        .entry(Kind::Distribution)
                        .and_modify(|v| *v += 1);
                }
                "ms" => {
                    msg_stats.kind.entry(Kind::Timer).and_modify(|v| *v += 1);
                }
                "g" => {
                    msg_stats.kind.entry(Kind::Gauge).and_modify(|v| *v += 1);
                }
                "c" => {
                    msg_stats.kind.entry(Kind::Count).and_modify(|v| *v += 1);
                }
                "s" => {
                    msg_stats.kind.entry(Kind::Set).and_modify(|v| *v += 1);
                }
                "h" => {
                    msg_stats
                        .kind
                        .entry(Kind::Histogram)
                        .and_modify(|v| *v += 1);
                }
                _ => {
                    if line.starts_with("_sc") {
                        msg_stats
                            .kind
                            .entry(Kind::ServiceCheck)
                            .and_modify(|v| *v += 1);
                    } else if line.starts_with("_e") {
                        msg_stats.kind.entry(Kind::Event).and_modify(|v| *v += 1);
                    } else {
                        println!("Found unknown msg type for dogstatsd msg: {}", line);
                    }
                }
            },
            _ => {
                println!("Found unusual dogstatsd msg: '{}'", line);
            }
        };

        line.clear();
    }

    Ok(msg_stats)
}
