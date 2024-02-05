use std::{
    collections::{hash_map::RandomState, BTreeSet, HashMap, HashSet},
    hash::{BuildHasher, Hasher},
    io::Write,
};

use histo::Histogram;
use lading_payload::dogstatsd::ValueConf;

use crate::{
    dogstatsdmsg::{DogStatsDMetricType, DogStatsDMsg, DogStatsDMsgKind},
    dogstatsdreader::DogStatsDReader,
};

const DEFAULT_NUM_BUCKETS: u64 = 10;

pub fn histo_min_max(histo: &histo::Histogram) -> (u64, u64) {
    let min = histo.buckets().filter(|bucket| bucket.count() > 0).map(|bucket| bucket.start()).min().unwrap_or_default();
    let max = histo.buckets().filter(|bucket| bucket.count() > 0).map(|bucket| bucket.end()).max().unwrap_or_default();
    (min, max)
}

fn get_metric_weights(batch: &DogStatsDBatchStats) -> lading_payload::dogstatsd::MetricWeights {
    // metric weights
    let metric_map = match batch.kind.get(&DogStatsDMsgKind::Metric) {
        Some((_, Some(m))) => m,
        _ => return lading_payload::dogstatsd::MetricWeights::default(),
    };

    let num_count = *metric_map.get(&DogStatsDMetricType::Count).unwrap_or(&0);
    let num_gauge = *metric_map.get(&DogStatsDMetricType::Gauge).unwrap_or(&0);
    let num_set = *metric_map.get(&DogStatsDMetricType::Set).unwrap_or(&0);
    let num_timer = *metric_map.get(&DogStatsDMetricType::Timer).unwrap_or(&0);
    let num_histogram = *metric_map.get(&DogStatsDMetricType::Histogram).unwrap_or(&0);
    let num_distribution = *metric_map.get(&DogStatsDMetricType::Distribution).unwrap_or(&0);

    let scale_factor = (num_count + num_gauge + num_set + num_timer + num_histogram + num_distribution) as f32 / u8::MAX as f32;
    let num_count = (num_count as f32 / scale_factor).round() as u8;
    let num_gauge = (num_gauge as f32 / scale_factor).round() as u8;
    let num_set = (num_set as f32 / scale_factor).round() as u8;
    let num_timer = (num_timer as f32 / scale_factor).round() as u8;
    let num_histogram = (num_histogram as f32 / scale_factor).round() as u8;
    let num_distribution = (num_distribution as f32 / scale_factor).round() as u8;

    lading_payload::dogstatsd::MetricWeights::new(num_count, num_gauge, num_set, num_timer, num_histogram, num_distribution)
}

type KindCount = (u32, Option<HashMap<DogStatsDMetricType, u32>>);
type KindMap = HashMap<DogStatsDMsgKind, KindCount>;

pub struct DogStatsDBatchStats {
    pub name_length: Histogram,
    pub num_values: Histogram,
    pub num_tags: Histogram,
    pub tag_total_length: Histogram,
    pub num_unicode_tags: Histogram,
    pub kind: KindMap,
    pub num_contexts: u32,
    pub total_unique_tags: u32,
}

impl DogStatsDBatchStats {
    pub fn to_lading_config(&self) -> lading_payload::dogstatsd::Config {
        let (min, max) = histo_min_max(&self.name_length);
        let num_contexts = lading_payload::dogstatsd::ConfRange::Constant(self.num_contexts);
        let name_length = lading_payload::dogstatsd::ConfRange::Inclusive{ min: min as u16, max: max as u16 };

        let (min, max) = histo_min_max(&self.tag_total_length);
        let tag_key_length = lading_payload::dogstatsd::ConfRange::Inclusive{min: min as u8, max: max as u8};
        let tag_value_length = lading_payload::dogstatsd::ConfRange::Inclusive{min: min as u8, max: max as u8};

        // num_values is non-zero when there is more than one value present
        // so to calculate the multivalue-pack-probability, its just the number of
        // non-zero values divided by the total number of values
        let mut zero_count = 0;
        let mut non_zero_count = 0;
        for bucket in self.num_values.buckets() {
            if bucket.start() == 0 && bucket.end() > 0 {
                zero_count = bucket.count();
            } else {
                non_zero_count += bucket.count();
            }
        }
        let multivalue_pack_probability = non_zero_count as f32 / (zero_count + non_zero_count) as f32;

        // kind weights
        let num_metrics = match self.kind.get(&DogStatsDMsgKind::Metric) {
            Some((v, _)) => *v,
            None => 0,
        };
        let num_events = match self.kind.get(&DogStatsDMsgKind::Event) {
            Some((v, _)) => *v,
            None => 0,
        };
        let num_service_checks = match self.kind.get(&DogStatsDMsgKind::ServiceCheck) {
            Some((v, _)) => *v,
            None => 0,
        };

        let scale_factor = (num_metrics + num_events + num_service_checks) as f32 / u8::MAX as f32;

        let num_metrics = (num_metrics as f32 / scale_factor).round() as u8;
        let num_events = (num_events as f32 / scale_factor).round() as u8;
        let num_service_checks = (num_service_checks as f32 / scale_factor).round() as u8;

        let kind_weights = lading_payload::dogstatsd::KindWeights::new(num_metrics, num_events, num_service_checks);

        let metric_weights = get_metric_weights(self);

        lading_payload::dogstatsd::Config {
            contexts: num_contexts,
            kind_weights,
            service_check_names: lading_payload::dogstatsd::ConfRange::Constant(0),// todo
            name_length,
            tag_key_length,
            tag_value_length,
            tags_per_msg: lading_payload::dogstatsd::ConfRange::Constant(0),// todo
            multivalue_pack_probability,
            multivalue_count: lading_payload::dogstatsd::ConfRange::Constant(0),// todo
            length_prefix_framed: false,
            sampling_range: lading_payload::dogstatsd::ConfRange::Constant(0.0),// todo
            sampling_probability: 0.0, // todo
            metric_weights,
            value: ValueConf::default(),
        }
    }
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


pub fn analyze_msgs(
    reader: &mut DogStatsDReader,
) -> Result<DogStatsDBatchStats, std::io::Error>
{
    let default_num_buckets = DEFAULT_NUM_BUCKETS;
    let mut msg_stats = DogStatsDBatchStats {
        name_length: Histogram::with_buckets(default_num_buckets),
        num_values: Histogram::with_buckets(default_num_buckets),
        num_tags: Histogram::with_buckets(default_num_buckets),
        tag_total_length: Histogram::with_buckets(default_num_buckets),
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
    loop {
        line.clear();
        let Ok(num_read) = reader.read_msg(&mut line) else {
            break;
        };
        if num_read == 0 {
            // EOF
            break;
        }
        let metric_msg = match DogStatsDMsg::new(&line) {
            Ok(DogStatsDMsg::Metric(m)) => m,
            Ok(DogStatsDMsg::Event(_)) => {
                msg_stats
                    .kind
                    .entry(DogStatsDMsgKind::Event)
                    .and_modify(|(v, _)| *v += 1);
                continue;
            }
            Ok(DogStatsDMsg::ServiceCheck(_)) => {
                msg_stats
                    .kind
                    .entry(DogStatsDMsgKind::ServiceCheck)
                    .and_modify(|(v, _)| *v += 1);
                continue;
            }
            Err(e) => {
                println!("Error parsing dogstatsd msg: {}", e);
                continue;
            }
        };
        let num_values = metric_msg.values.split(':').count() as u64;

        let mut num_unicode_tags = 0;
        let num_tags = metric_msg.tags.len() as u64;
        for tag in &metric_msg.tags {
            msg_stats.tag_total_length.add(tag.len() as u64);
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
    }

    msg_stats.total_unique_tags = tags_seen.len() as u32;
    msg_stats.num_contexts = context_map.len() as u32;
    Ok(msg_stats)
}

#[cfg(test)]
mod tests {





    use super::*;

    #[test]
    fn counting_contexts() {
        let payload = b"my.metric:1|g\nmy.metric:2|g\nother.metric:20|d|#env:staging\nother.thing:10|d|#datacenter:prod\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 3);
    }

    #[test]
    fn counting_contexts_name_variations() {
        let payload =
            b"my.metrice:1|g\nmy.metricd:1|g\nmy.metricc:1|g\nmy.metricb:1|g\nmy.metrica:1|g\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);

        let payload =
            b"my.metric:1|g|#foo:a\nmy.metric:1|g\nmy.metric:1|g\nmy.metric:1|g\nmy.metric:1|g\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 2);
    }

    #[test]
    fn counting_contexts_tag_variations() {
        let payload =
            b"my.metric:1|g|#foo:a\nmy.metric:1|g|#foo:b\nmy.metric:1|g|#foo:c\nmy.metric:1|g|#foo:d\nmy.metric:1|g|#foo:e\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);

        let payload =
            b"my.metric:1|g|#a:foo\nmy.metric:1|g|#b:foo\nmy.metric:1|g|#c:foo\nmy.metric:1|g|#d:foo\nmy.metric:1|g|#e:foo\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);

        let payload =
            b"my.metric:1|g|#foo\nmy.metric:1|g|#b:foo\nmy.metric:1|g|#b:foo\nmy.metric:1|g|#d:foo\nmy.metric:1|g|#e:foo\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 4);
    }

    #[test]
    fn counting_contexts_tag_order() {
        let payload =
            b"my.metric:1|g|#foo:a,b,c,d,e\nmy.metric:1|g|#foo:b,a,c,d,e\nmy.metric:1|g|#foo:c,a,b,d,e\nmy.metric:1|g|#foo:d,a,b,c,e\nmy.metric:1|g|#foo:e,a,b,c,d\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);

        let payload =
            b"my.metric:1|g|#a:foo,b,c,d,e\nmy.metric:1|g|#b:foo,a,c,d,e\nmy.metric:1|g|#c:foo,a,b,d,e\nmy.metric:1|g|#d:foo,a,b,c,e\nmy.metric:1|g|#e:foo,a,b,c,d\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 5);
    }

    // Generate me tests that will use varying numbers of tags
    #[test]
    fn counting_contexts_tag_count() {
        let payload =
            b"my.metric:1|g|#foo:a,b,c,d,e\nmy.metric:1|g|#foo:b,a,c,d\nmy.metric:1|g|#foo:c,a,b\nmy.metric:1|g|#foo:d,a\nmy.metric:1|g|#foo:e\nmy.metric:1|g\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 6);

        let payload =
            b"my.metric:1|g|#a:foo,b,c,d,e\nmy.metric:1|g|#b:foo,a,c,d\nmy.metric:1|g|#c:foo,a,b\nmy.metric:1|g|#d:foo,a\nmy.metric:1|g|#e:foo\nmy.metric:1|g\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        assert_eq!(res.num_contexts, 6);
    }

    #[test]
    fn counting_contexts_tag_value_length() {
        let payload =
            b"my.metric:1|g|#foo:aaaaaaaaaaaaaa,bbbbbbbbbbbbbb,cccccccccccccc,dddddddddddddd,eeeeeeeeeeeeee\nmy.metric:1|g|#foo:bbbbbbbbbbbbbb,aaaaaaaaaaaaaa,cccccccccccccc,dddddddddddddd\nmy.metric:1|g|#foo:cccccccccccccc,aaaaaaaaaaaaaa,bbbbbbbbbbbbbb\nmy.metric:1|g|#foo:dddddddddddddd,aaaaaaaaaaaaaa\nmy.metric:1|g|#foo:eeeeeeeeeeeeee\nmy.metric:1|g\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();

        // 6 because of the empty tags
        assert_eq!(res.num_contexts, 6);
    }

    #[test]
    fn batch_stats_to_lading_config() {
        let mut stats = DogStatsDBatchStats {
            name_length: Histogram::with_buckets(10),
            num_tags: Histogram::with_buckets(10),
            tag_total_length: Histogram::with_buckets(10),
            num_unicode_tags: Histogram::with_buckets(10),
            kind: HashMap::new(),
            total_unique_tags: 0,
            num_contexts: 0,
            num_values: Histogram::with_buckets(10),
        };

        stats.name_length.add(10);
        stats.name_length.add(10);
        stats.name_length.add(10);
        stats.name_length.add(10);

        let lading_config = stats.to_lading_config();
        // This currently fails because the bucket range is 10-11, so the max is 11
        // even though there are no samples at this value.
        // This strikes me as an indication that I have outgrown the histo::histogram crate
        // lets try dd-sketch
        assert_eq!(lading_config.name_length, lading_payload::dogstatsd::ConfRange::Inclusive{min: 10, max: 10});
    }


}
