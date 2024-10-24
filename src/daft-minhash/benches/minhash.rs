use std::{collections::VecDeque, hash::BuildHasher, iter::repeat_with};

use daft_hash::MurBuildHasher;
use daft_minhash::{load_simd, minhash_in};
use tango_bench::{
    benchmark_fn, tango_benchmarks, tango_main, Benchmark, IntoBenchmarks, MeasurementSettings,
    DEFAULT_SETTINGS,
};
use xxhash_rust::{xxh3::Xxh3DefaultBuilder, xxh64::Xxh64Builder};

const N_TOKENS: usize = 10000;
const N_CHARS_MIN: usize = 1;
const N_CHARS_MAX: usize = 20;
const NUM_HASHES: usize = 128;
const NGRAM_SIZE: usize = 13;

fn generate_input(seed: u64) -> String {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut s = String::new();
    for i in 0..N_TOKENS {
        if i > 0 {
            s.push(' ');
        }
        let s_chars = rng.usize(N_CHARS_MIN..N_CHARS_MAX);
        for _ in 0..s_chars {
            s.push(rng.alphanumeric());
        }
    }
    s
}

fn bench_minhash_with_hasher<H: BuildHasher + Default + 'static>(name: &'static str) -> Benchmark {
    benchmark_fn(format!("minhash/{name}"), move |b| {
        let mut rng = fastrand::Rng::with_seed(b.seed);

        // Generate permutations
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64)))
            .take(NUM_HASHES)
            .collect::<Vec<_>>();
        let perm_a_simd = load_simd(perm_a, NUM_HASHES);

        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64)))
            .take(NUM_HASHES)
            .collect::<Vec<_>>();
        let perm_b_simd = load_simd(perm_b, NUM_HASHES);

        // Generate input string
        let input = generate_input(b.seed);

        let mut vec = VecDeque::new();

        b.iter(move || {
            minhash_in(
                &input,
                (&perm_a_simd, &perm_b_simd),
                NUM_HASHES,
                NGRAM_SIZE,
                &H::default(),
                &mut vec,
            )
        })
    })
}

fn all_benchmarks() -> impl IntoBenchmarks {
    [
        bench_minhash_with_hasher::<MurBuildHasher>("mur_hasher"),
        bench_minhash_with_hasher::<Xxh3DefaultBuilder>("xxh3_hasher"),
        bench_minhash_with_hasher::<Xxh64Builder>("xxh64_hasher"),
    ]
}

// Customized settings for stable measurements
const SETTINGS: MeasurementSettings = MeasurementSettings {
    // Increase minimum iterations for more stable results
    min_iterations_per_sample: 1000,
    // Enable cache firewall to reduce cache effects
    cache_firewall: Some(64), // 64KB cache firewall
    // Enable yielding to reduce scheduler effects
    yield_before_sample: true,
    // Enable stack randomization to reduce alignment effects
    randomize_stack: Some(4096), // 4KB stack randomization
    // Rest of settings from default
    ..DEFAULT_SETTINGS
};

tango_benchmarks!(all_benchmarks());
tango_main!(SETTINGS);
