use sketches_ddsketch::{Config, DDSketch};

use std::{
    collections::{hash_map::RandomState, BTreeSet, HashMap, HashSet},
    hash::{BuildHasher, Hasher},
    io::Write,
};

use thiserror::Error;
use lading_payload::dogstatsd::{ValueConf, KindWeights, MetricWeights};

use crate::{
    dogstatsdmsg::{DogStatsDMetricType, DogStatsDMsg, DogStatsDMsgKind},
    dogstatsdreader::DogStatsDReader,
};

type KindCount = (u32, Option<HashMap<DogStatsDMetricType, u32>>);
type KindMap = HashMap<DogStatsDMsgKind, KindCount>;

pub struct DogStatsDBatchStats {
    pub name_length: DDSketch,
    pub num_values: DDSketch,
    pub num_tags: DDSketch,
    pub tag_total_length: DDSketch,
    pub num_unicode_tags: DDSketch,
    pub kind: KindMap,
    pub num_contexts: u32,
    pub total_unique_tags: u32,
    pub num_msgs_with_multivalue: u32,
    pub num_msgs: u32,
    pub reader_analytics: Option<crate::dogstatsdreader::Analytics>,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error retrieving data from sketch: {0}")]
    DDSketchError(#[from] sketches_ddsketch::DDSketchError),
}

impl DogStatsDBatchStats {
    fn get_metric_weights(&self) -> MetricWeights {
        // metric weights
        let (total_metrics, metric_map) = match self.kind.get(&DogStatsDMsgKind::Metric) {
            Some((total_count, Some(map))) => (total_count, map),
            _ => return lading_payload::dogstatsd::MetricWeights::default(),
        };

        if *total_metrics == 0 {
            return lading_payload::dogstatsd::MetricWeights::default();
        }

        let num_count = *metric_map.get(&DogStatsDMetricType::Count).unwrap_or(&0);
        let num_gauge = *metric_map.get(&DogStatsDMetricType::Gauge).unwrap_or(&0);
        let num_set = *metric_map.get(&DogStatsDMetricType::Set).unwrap_or(&0);
        let num_timer = *metric_map.get(&DogStatsDMetricType::Timer).unwrap_or(&0);
        let num_histogram = *metric_map.get(&DogStatsDMetricType::Histogram).unwrap_or(&0);
        let num_distribution = *metric_map.get(&DogStatsDMetricType::Distribution).unwrap_or(&0);

        if *total_metrics < u8::MAX as u32 {
            return lading_payload::dogstatsd::MetricWeights::new(
                num_count as u8,
                num_gauge as u8,
                num_timer as u8,
                num_distribution as u8,
                num_set as u8,
                num_histogram as u8,
            );
        }

        let scale_factor = (num_count + num_gauge + num_set + num_timer + num_histogram + num_distribution) as f32 / u8::MAX as f32;
        let num_count = (num_count as f32 / scale_factor).round() as u8;
        let num_gauge = (num_gauge as f32 / scale_factor).round() as u8;
        let num_set = (num_set as f32 / scale_factor).round() as u8;
        let num_timer = (num_timer as f32 / scale_factor).round() as u8;
        let num_histogram = (num_histogram as f32 / scale_factor).round() as u8;
        let num_distribution = (num_distribution as f32 / scale_factor).round() as u8;

        lading_payload::dogstatsd::MetricWeights::new(num_count, num_gauge, num_timer, num_distribution, num_set, num_histogram)
    }

    fn get_kind_weights(&self) -> KindWeights {
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

        lading_payload::dogstatsd::KindWeights::new(num_metrics, num_events, num_service_checks)
    }

    /// Given a DogStatsDBatchStats, return a lading_payload::dogstatsd::Config
    /// Correctly populates all payload parameters except for sampling
    pub fn to_lading_config(&self) -> Result<lading_payload::dogstatsd::Config, Error> {
        // could use min-max here, but I'm thinking that getting the 20th and 80th percentiles
        // may be more useful than the absolute min and max
        let name_length = if let (Some(min), Some(max)) = (self.name_length.quantile(0.2)?, self.name_length.quantile(0.8)?) {
            lading_payload::dogstatsd::ConfRange::Inclusive{min: min as u16, max: max as u16}
        } else {
            // todo, how to re-use default from lading_payload::dogstatsd
            lading_payload::dogstatsd::ConfRange::Constant(10)
        };
        let num_contexts = lading_payload::dogstatsd::ConfRange::Constant(self.num_contexts);

        let tag_length = if let (Some(min), Some(max)) = (self.tag_total_length.quantile(0.2)?, self.tag_total_length.quantile(0.8)?) {
            lading_payload::dogstatsd::ConfRange::Inclusive{min: min as u8, max: max as u8}
        } else {
            // todo, how to re-use default from lading_payload::dogstatsd
            lading_payload::dogstatsd::ConfRange::Constant(20)
        };
        let tag_key_length = tag_length;
        let tag_value_length = tag_length;
        let tags_per_msg = if let (Some(min), Some(max)) = (self.num_tags.quantile(0.2)?, self.num_tags.quantile(0.8)?) {
            lading_payload::dogstatsd::ConfRange::Inclusive{min: min as u8, max: max as u8}
        } else {
            lading_payload::dogstatsd::ConfRange::Constant(0)
        };

        let multivalue_count = if let (Some(min), Some(max)) = (self.num_values.quantile(0.2)?, self.num_values.quantile(0.8)? ) {
            lading_payload::dogstatsd::ConfRange::Inclusive{min: min as u16, max: max as u16}
        } else {
            lading_payload::dogstatsd::ConfRange::Constant(0)
        };

        let multivalue_pack_probability = self.num_msgs_with_multivalue as f32 / (self.num_msgs) as f32;

        let kind_weights = self.get_kind_weights();
        let metric_weights = self.get_metric_weights();

        Ok(lading_payload::dogstatsd::Config {
            contexts: num_contexts,
            kind_weights,
            service_check_names: name_length, // todo, track name length for service checks specifically
            name_length,
            tag_key_length,
            tag_value_length,
            tags_per_msg,
            multivalue_pack_probability,
            multivalue_count,
            length_prefix_framed: false,
            // Sampling is not yet implemented, neither in DogStatsDMsg nor in the analysis code
            // todo, implement sampling analysis
            sampling_range: lading_payload::dogstatsd::ConfRange::Constant(0.0),
            sampling_probability: 0.0,
            metric_weights,
            value: ValueConf::default(),
        })
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
    let default_config = Config::defaults();
    let mut msg_stats = DogStatsDBatchStats {
        name_length: DDSketch::new(default_config),
        num_values: DDSketch::new(default_config),
        num_tags: DDSketch::new(default_config),
        tag_total_length: DDSketch::new(default_config),
        num_unicode_tags: DDSketch::new(default_config),
        kind: HashMap::new(),
        total_unique_tags: 0,
        num_contexts: 0,
        num_msgs: 0,
        num_msgs_with_multivalue: 0,
        reader_analytics: None,
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
        msg_stats.num_msgs += 1;
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
        // todo push this down into DogStatsDMsg
        let num_values = metric_msg.values.split(':').count() as f64;

        let mut num_unicode_tags = 0_f64;
        let num_tags = metric_msg.tags.len() as f64;
        for tag in &metric_msg.tags {
            msg_stats.tag_total_length.add(tag.len() as f64);
            tags_seen.insert(tag.to_string());
            if !tag.is_ascii() {
                num_unicode_tags += 1.0;
            }
        }

        msg_stats.name_length.add(metric_msg.name.len() as f64);
        msg_stats.num_tags.add(num_tags);
        msg_stats.num_unicode_tags.add(num_unicode_tags);
        msg_stats.num_values.add(num_values);
        if num_values > 1.0 {
            msg_stats.num_msgs_with_multivalue += 1;
        }

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

    // Have read through the entire reader, lets try to grab the final "Analytics" if it exists
    msg_stats.reader_analytics = reader.get_analytics().expect("Error getting analytics from reader");
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
        let config  = Config::defaults();
        let mut stats = DogStatsDBatchStats {
            name_length: DDSketch::new(config),
            num_tags: DDSketch::new(config),
            tag_total_length: DDSketch::new(config),
            num_unicode_tags: DDSketch::new(config),
            kind: HashMap::new(),
            total_unique_tags: 0,
            num_contexts: 0,
            num_values: DDSketch::new(config),
            num_msgs: 4,
            num_msgs_with_multivalue: 0,
        };

        stats.name_length.add(10.0);
        stats.name_length.add(10.0);
        stats.name_length.add(10.0);
        stats.name_length.add(10.0);

        let lading_config = stats.to_lading_config().unwrap();
        assert_eq!(lading_config.name_length, lading_payload::dogstatsd::ConfRange::Inclusive{min: 10, max: 10});
    }

    #[test]
    fn stats_lading_metric_weights() {
        let payload =
            b"my.metric:1|g\nmy.metric:2|g\nother.metric:20|d|#env:staging\nother.thing:10|d|#datacenter:prod\n";
        let mut reader = DogStatsDReader::new(&payload[..]).unwrap();
        let res = analyze_msgs(&mut reader).unwrap();
        let lading_config = res.to_lading_config().unwrap();

        assert_eq!(lading_config.metric_weights, lading_payload::dogstatsd::MetricWeights::new(0, 2, 0, 2, 0, 0));
    }

    #[test]
    fn metric_weight_scale() {
        let config  = Config::defaults();
        let mut stats = DogStatsDBatchStats {
            name_length: DDSketch::new(config),
            num_tags: DDSketch::new(config),
            tag_total_length: DDSketch::new(config),
            num_unicode_tags: DDSketch::new(config),
            kind: HashMap::new(),
            total_unique_tags: 0,
            num_contexts: 0,
            num_values: DDSketch::new(config),
            num_msgs: 4,
            num_msgs_with_multivalue: 0,
        };

        let mut metric_map = HashMap::new();
        metric_map.insert(DogStatsDMetricType::Count, 2);
        metric_map.insert(DogStatsDMetricType::Distribution, 2);
        stats.kind.insert(DogStatsDMsgKind::Metric, (4, Some(metric_map)));

        let metric_weights = stats.get_metric_weights();

        assert_eq!(metric_weights, lading_payload::dogstatsd::MetricWeights::new(2, 0, 0, 2, 0, 0));

        let mut metric_map = HashMap::new();
        metric_map.insert(DogStatsDMetricType::Count, 200);
        metric_map.insert(DogStatsDMetricType::Distribution, 200);
        stats.kind.insert(DogStatsDMsgKind::Metric, (400, Some(metric_map)));

        let metric_weights = stats.get_metric_weights();

        assert_eq!(metric_weights, lading_payload::dogstatsd::MetricWeights::new(128, 0, 0, 128, 0, 0));
    }
}
