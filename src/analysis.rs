use std::{
    collections::{hash_map::RandomState, BTreeSet, HashMap, HashSet},
    fmt::Display,
    hash::{BuildHasher, Hasher},
    io::Write,
};

use histo::Histogram;

use crate::{dogstatsdmsg::DogStatsDStr, dogstatsdreader::DogStatsDReader};

const DEFAULT_NUM_BUCKETS: u64 = 10;

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
    pub kind: HashMap<Kind, u32>,
    pub num_contexts: u32,
    pub total_unique_tags: u32,
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
    let default_num_buckets = DEFAULT_NUM_BUCKETS;
    let mut msg_stats = DogStatsDBatchStats {
        name_length: Histogram::with_buckets(default_num_buckets),
        num_values: Histogram::with_buckets(default_num_buckets),
        num_tags: Histogram::with_buckets(default_num_buckets),
        num_unicode_tags: Histogram::with_buckets(default_num_buckets),
        kind: HashMap::new(),
        total_unique_tags: 0,
        num_contexts: 0,
    };

    msg_stats.kind.insert(Kind::Count, 0);
    msg_stats.kind.insert(Kind::Distribution, 0);
    msg_stats.kind.insert(Kind::Event, 0);
    msg_stats.kind.insert(Kind::Gauge, 0);
    msg_stats.kind.insert(Kind::Histogram, 0);
    msg_stats.kind.insert(Kind::ServiceCheck, 0);
    msg_stats.kind.insert(Kind::Set, 0);

    let mut tags_seen: HashSet<String> = HashSet::new();
    let mut line = String::new();
    let mut context_map: HashMap<u64, u64> = HashMap::new();
    let hash_builder = RandomState::new();
    while let Ok(num_read) = reader.read_msg(&mut line) {
        if num_read == 0 {
            // EOF
            break;
        }
        let metric_msg = match DogStatsDStr::new(&line) {
            Ok(DogStatsDStr::Metric(m)) => m,
            Ok(DogStatsDStr::Event(_)) => {
                msg_stats.kind.entry(Kind::Event).and_modify(|v| *v += 1);
                line.clear();
                continue;
            }
            Ok(DogStatsDStr::ServiceCheck(_)) => {
                msg_stats
                    .kind
                    .entry(Kind::ServiceCheck)
                    .and_modify(|v| *v += 1);
                line.clear();
                continue;
            }
            Err(e) => {
                println!("Error parsing dogstatsd msg: {}", e);
                line.clear();
                continue;
            }
        };
        let num_values = metric_msg.values.split(':').count() as u64;

        let mut num_unicode_tags = 0;
        let num_tags = metric_msg.tags.len() as u64;
        for tag in &metric_msg.tags {
            tags_seen.insert(tag.to_string());
            if !tag.is_ascii() {
                num_unicode_tags += 1;
            }
        }

        msg_stats.name_length.add(metric_msg.name.len() as u64);
        msg_stats.num_tags.add(num_tags);
        msg_stats.num_unicode_tags.add(num_unicode_tags);
        msg_stats.num_values.add(num_values);

        let mut metric_context = hash_builder.build_hasher();
        metric_context.write_usize(metric_msg.name.len());
        metric_context.write(metric_msg.name.as_bytes());
        // Use a BTreeSet to ensure that the tags are sorted
        let labels: BTreeSet<&&str> = metric_msg.tags.iter().collect();
        let metric_context = labels
            .iter()
            .fold(metric_context, |mut hasher, t| {
                hasher.write_usize(t.len());
                hasher.write(t.as_bytes());
                hasher
            })
            .finish();
        let context_entry = context_map.entry(metric_context).or_default();
        *context_entry += 1;

        match metric_msg.metric_type {
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
                println!("Found unknown msg type for dogstatsd msg: {}", line);
            }
        }

        line.clear();
    }

    msg_stats.total_unique_tags = tags_seen.len() as u32;
    msg_stats.num_contexts = context_map.len() as u32;
    Ok(msg_stats)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn two_msg_two_lines() {
        let payload = b"my.metric:1|g\nmy.metric:2|g\nother.metric:20|d|#env:staging\nother.thing:10|d|#datacenter:prod\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        // TODO not implemented yet
        // assert_eq!(res.num_contexts, 3);
    }
}
