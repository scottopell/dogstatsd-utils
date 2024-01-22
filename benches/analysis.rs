use std::time::Duration;

use divan::counter::BytesCount;
use dogstatsd_utils::{
    analysis::analyze_msgs, dogstatsdreader::DogStatsDReader,
};
use lading_payload::dogstatsd::{self, KindWeights, MetricWeights, ValueConf};
use rand::{rngs::SmallRng, SeedableRng};

fn main() {
    // Run registered benchmarks.
    divan::main();
}

#[divan::bench(min_time = Duration::from_secs(10))]
fn analysis_throughput(bencher: divan::Bencher) {
    let mut rng = SmallRng::seed_from_u64(34512423); // todo use random seed
    let length_prefix_framed = false;
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
        length_prefix_framed,
        &mut rng,
    )
    .expect("Failed to create dogstatsd generator");

    bencher
        .with_inputs(|| {
            format!("{}", dd.generate(&mut rng)).into_bytes()
        })
        .input_counter(|payload| {
            // Changes based on input.
            BytesCount::usize(payload.len())
        })
        .bench_local_refs(|payload| {
            let cursor = std::io::Cursor::new(payload);
            let mut reader = DogStatsDReader::new(cursor).unwrap();
            analyze_msgs(&mut reader).unwrap();
        })
}
