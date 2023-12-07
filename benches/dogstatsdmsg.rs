use std::time::Duration;

use divan::counter::BytesCount;
use dogstatsd_utils::dogstatsdmsg::DogStatsDStr;
use lading_payload::dogstatsd::{self, KindWeights, MetricWeights, ValueConf};
use rand::{rngs::SmallRng, SeedableRng};

fn main() {
    // Run registered benchmarks.
    divan::main();
}

#[divan::bench]
fn dogstatsdmsg_parsing() {
    fn compute(n: u64) {
        for msg in vec!["my.metric:1|g#some:tag"].repeat(n as usize) {
            let msg = DogStatsDStr::new(msg);
            let _ = msg;
        }
    }

    compute(divan::black_box(10))
}

#[divan::bench(min_time = Duration::from_secs(10))]
fn dogstatsdmsg_parsing_throughput(bencher: divan::Bencher) {
    let mut rng = SmallRng::seed_from_u64(34512423); // todo use random seed
    let dd = dogstatsd::DogStatsD::new(
        // Contexts
        dogstatsd::ConfRange::Inclusive {
            min: 500,
            max: 10000,
        },
        // Service check name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_key_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_value_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tags_per_msg
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_count
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_pack_probability
        0.08,
        // sample_rate_range
        dogstatsd::ConfRange::Inclusive { min: 0.1, max: 1.0 },
        // sample_rate_choose_probability
        0.50,
        KindWeights::default(),
        MetricWeights::default(),
        ValueConf::default(),
        &mut rng,
    )
    .expect("Failed to create dogstatsd generator");

    bencher
        .with_inputs(|| format!("{}", dd.generate(&mut rng)))
        .input_counter(|s: &String| {
            // Changes based on input.
            BytesCount::of_str(s)
        })
        .bench_local_values(|s: String| {
            let msg = DogStatsDStr::new(s.as_str());
            let _ = msg;
        })
}

#[divan::bench(min_time = Duration::from_secs(2))]
fn dogstatsdmsg_parsing_metrics_only_throughput(bencher: divan::Bencher) {
    let mut rng = SmallRng::seed_from_u64(34512423); // todo use random seed
    let kind_weights = KindWeights::new(1, 0, 0);
    let dd = dogstatsd::DogStatsD::new(
        // Contexts
        dogstatsd::ConfRange::Inclusive {
            min: 500,
            max: 10000,
        },
        // Service check name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_key_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_value_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tags_per_msg
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_count
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_pack_probability
        0.08,
        // sample_rate_range
        dogstatsd::ConfRange::Inclusive { min: 0.1, max: 1.0 },
        // sample_rate_choose_probability
        0.50,
        kind_weights,
        MetricWeights::default(),
        ValueConf::default(),
        &mut rng,
    )
    .expect("Failed to create dogstatsd generator");

    bencher
        .with_inputs(|| format!("{}", dd.generate(&mut rng)))
        .input_counter(|s: &String| {
            // Changes based on input.
            BytesCount::of_str(s)
        })
        .bench_local_values(|s: String| {
            let msg = DogStatsDStr::new(s.as_str());
            let _ = msg;
        })
}

#[divan::bench(min_time = Duration::from_secs(2))]
fn dogstatsdmsg_parsing_events_only_throughput(bencher: divan::Bencher) {
    let mut rng = SmallRng::seed_from_u64(34512423); // todo use random seed
    let kind_weights = KindWeights::new(0, 1, 0);
    let dd = dogstatsd::DogStatsD::new(
        // Contexts
        dogstatsd::ConfRange::Inclusive {
            min: 500,
            max: 10000,
        },
        // Service check name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_key_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_value_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tags_per_msg
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_count
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_pack_probability
        0.08,
        // sample_rate_range
        dogstatsd::ConfRange::Inclusive { min: 0.1, max: 1.0 },
        // sample_rate_choose_probability
        0.50,
        kind_weights,
        MetricWeights::default(),
        ValueConf::default(),
        &mut rng,
    )
    .expect("Failed to create dogstatsd generator");

    bencher
        .with_inputs(|| format!("{}", dd.generate(&mut rng)))
        .input_counter(|s: &String| {
            // Changes based on input.
            BytesCount::of_str(s)
        })
        .bench_local_values(|s: String| {
            let msg = DogStatsDStr::new(s.as_str());
            let _ = msg;
        })
}

#[divan::bench(min_time = Duration::from_secs(2))]
fn dogstatsdmsg_parsing_servicechecks_only_throughput(bencher: divan::Bencher) {
    let mut rng = SmallRng::seed_from_u64(34512423); // todo use random seed
    let kind_weights = KindWeights::new(0, 0, 1);
    let dd = dogstatsd::DogStatsD::new(
        // Contexts
        dogstatsd::ConfRange::Inclusive {
            min: 500,
            max: 10000,
        },
        // Service check name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // name length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_key_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tag_value_length
        dogstatsd::ConfRange::Inclusive { min: 5, max: 10 },
        // tags_per_msg
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_count
        dogstatsd::ConfRange::Inclusive { min: 1, max: 10 },
        // multivalue_pack_probability
        0.08,
        // sample_rate_range
        dogstatsd::ConfRange::Inclusive { min: 0.1, max: 1.0 },
        // sample_rate_choose_probability
        0.50,
        kind_weights,
        MetricWeights::default(),
        ValueConf::default(),
        &mut rng,
    )
    .expect("Failed to create dogstatsd generator");

    bencher
        .with_inputs(|| format!("{}", dd.generate(&mut rng)))
        .input_counter(|s: &String| {
            // Changes based on input.
            BytesCount::of_str(s)
        })
        .bench_local_values(|s: String| {
            let msg = DogStatsDStr::new(s.as_str());
            let _ = msg;
        })
}
