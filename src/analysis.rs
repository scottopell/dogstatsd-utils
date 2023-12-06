use std::{
    collections::{hash_map::RandomState, BTreeSet, HashMap, HashSet},
    hash::{BuildHasher, Hasher},
    io::Write,
};

use histo::Histogram;

use crate::{
    dogstatsdmsg::{DogStatsDMetricType, DogStatsDMsgKind, DogStatsDStr},
    dogstatsdreader::DogStatsDReader,
};

const DEFAULT_NUM_BUCKETS: u64 = 10;

pub struct DogStatsDBatchStats {
    pub name_length: Histogram,
    pub num_values: Histogram,
    pub num_tags: Histogram,
    pub num_unicode_tags: Histogram,
    pub kind: HashMap<DogStatsDMsgKind, (u32, Option<HashMap<DogStatsDMetricType, u32>>)>,
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

    let mut metric_type_map = HashMap::new();
    metric_type_map.insert(DogStatsDMetricType::Count, 0);
    metric_type_map.insert(DogStatsDMetricType::Gauge, 0);
    metric_type_map.insert(DogStatsDMetricType::Set, 0);
    metric_type_map.insert(DogStatsDMetricType::Timer, 0);
    metric_type_map.insert(DogStatsDMetricType::Histogram, 0);
    metric_type_map.insert(DogStatsDMetricType::Distribution, 0);

    msg_stats.kind.insert(DogStatsDMsgKind::Event, (0, None));
    msg_stats
        .kind
        .insert(DogStatsDMsgKind::ServiceCheck, (0, None));
    msg_stats
        .kind
        .insert(DogStatsDMsgKind::Metric, (0, Some(metric_type_map)));

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
                msg_stats
                    .kind
                    .entry(DogStatsDMsgKind::Event)
                    .and_modify(|(v, _)| *v += 1);
                line.clear();
                continue;
            }
            Ok(DogStatsDStr::ServiceCheck(_)) => {
                msg_stats
                    .kind
                    .entry(DogStatsDMsgKind::ServiceCheck)
                    .and_modify(|(v, _)| *v += 1);
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

        msg_stats
            .kind
            .entry(DogStatsDMsgKind::Metric)
            .and_modify(|(total, per_type)| {
                *total += 1;
                if let Some(per_type) = per_type {
                    per_type
                        .entry(metric_msg.metric_type)
                        .and_modify(|v| *v += 1);
                }
            });

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
    fn counting_contexts() {
        let payload = b"my.metric:1|g\nmy.metric:2|g\nother.metric:20|d|#env:staging\nother.thing:10|d|#datacenter:prod\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 3);
    }

    #[test]
    fn counting_contexts_name_variations() {
        let payload =
            b"my.metrice:1|g\nmy.metricd:1|g\nmy.metricc:1|g\nmy.metricb:1|g\nmy.metrica:1|g\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);

        let payload =
            b"my.metric:1|g|#foo:a\nmy.metric:1|g\nmy.metric:1|g\nmy.metric:1|g\nmy.metric:1|g\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 2);
    }

    #[test]
    fn counting_contexts_tag_variations() {
        let payload =
            b"my.metric:1|g|#foo:a\nmy.metric:1|g|#foo:b\nmy.metric:1|g|#foo:c\nmy.metric:1|g|#foo:d\nmy.metric:1|g|#foo:e\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);

        let payload =
            b"my.metric:1|g|#a:foo\nmy.metric:1|g|#b:foo\nmy.metric:1|g|#c:foo\nmy.metric:1|g|#d:foo\nmy.metric:1|g|#e:foo\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);

        let payload =
            b"my.metric:1|g|#foo\nmy.metric:1|g|#b:foo\nmy.metric:1|g|#b:foo\nmy.metric:1|g|#d:foo\nmy.metric:1|g|#e:foo\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 4);
    }

    #[test]
    fn counting_contexts_tag_order() {
        let payload =
            b"my.metric:1|g|#foo:a,b,c,d,e\nmy.metric:1|g|#foo:b,a,c,d,e\nmy.metric:1|g|#foo:c,a,b,d,e\nmy.metric:1|g|#foo:d,a,b,c,e\nmy.metric:1|g|#foo:e,a,b,c,d\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);

        let payload =
            b"my.metric:1|g|#a:foo,b,c,d,e\nmy.metric:1|g|#b:foo,a,c,d,e\nmy.metric:1|g|#c:foo,a,b,d,e\nmy.metric:1|g|#d:foo,a,b,c,e\nmy.metric:1|g|#e:foo,a,b,c,d\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);
    }

    // Generate me tests that will use varying numbers of tags
    #[test]
    fn counting_contexts_tag_count() {
        let payload =
            b"my.metric:1|g|#foo:a,b,c,d,e\nmy.metric:1|g|#foo:b,a,c,d\nmy.metric:1|g|#foo:c,a,b\nmy.metric:1|g|#foo:d,a\nmy.metric:1|g|#foo:e\nmy.metric:1|g\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 6);

        let payload =
            b"my.metric:1|g|#a:foo,b,c,d,e\nmy.metric:1|g|#b:foo,a,c,d\nmy.metric:1|g|#c:foo,a,b\nmy.metric:1|g|#d:foo,a\nmy.metric:1|g|#e:foo\nmy.metric:1|g\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 6);
    }

    #[test]
    fn counting_contexts_tag_value_length() {
        let payload =
            b"my.metric:1|g|#foo:aaaaaaaaaaaaaa,bbbbbbbbbbbbbb,cccccccccccccc,dddddddddddddd,eeeeeeeeeeeeee\nmy.metric:1|g|#foo:bbbbbbbbbbbbbb,aaaaaaaaaaaaaa,cccccccccccccc,dddddddddddddd\nmy.metric:1|g|#foo:cccccccccccccc,aaaaaaaaaaaaaa,bbbbbbbbbbbbbb\nmy.metric:1|g|#foo:dddddddddddddd,aaaaaaaaaaaaaa\nmy.metric:1|g|#foo:eeeeeeeeeeeeee\nmy.metric:1|g\n";
        let mut reader = DogStatsDReader::new(Bytes::from_static(payload));
        let res = analyze_msgs(&mut reader).unwrap();

        // 6 because of the empty tags
        assert_eq!(res.num_contexts, 6);
    }
}
