use std::{collections::VecDeque, hint::black_box};

use daft_minhash::windowed::WindowedWordsExt;
use tango_bench::{
    benchmark_fn, tango_benchmarks, tango_main, Benchmark, IntoBenchmarks, MeasurementSettings,
    DEFAULT_SETTINGS,
};
// Import the windowed words functionality

const SMALL_TEXT: &str = "The quick brown fox jumps over the lazy dog";
const MEDIUM_TEXT: &str = "The quick brown fox jumps over the lazy dog. A wonderful serenity \
    has taken possession of my entire soul, like these sweet mornings of spring which I enjoy \
    with my whole heart. I am alone, and feel the charm of existence in this spot, which was \
    created for the bliss of souls like mine.";
const LARGE_TEXT: &str = "The quick brown fox jumps over the lazy dog. A wonderful serenity \
    has taken possession of my entire soul, like these sweet mornings of spring which I enjoy \
    with my whole heart. I am alone, and feel the charm of existence in this spot, which was \
    created for the bliss of souls like mine. Far far away, behind the word mountains, far \
    from the countries Vokalia and Consonantia, there live the blind texts. Separated they \
    live in Bookmarksgrove right at the coast of the Semantics, a large language ocean. A \
    small river named Duden flows by their place and supplies it with the necessary regelialia.";

fn bench_windowed_words(text: &'static str, window_size: usize) -> Benchmark {
    benchmark_fn(
        format!(
            "windowed_words/text_len_{}/window_{}",
            text.len(),
            window_size
        ),
        move |b| {
            let mut vec = VecDeque::new();
            b.iter(move || {
                let iter = text.windowed_words_in(window_size, &mut vec);

                for elem in iter {
                    black_box(elem);
                }
            })
        },
    )
}

fn all_benchmarks() -> impl IntoBenchmarks {
    let mut benchmarks = Vec::new();

    // Test different window sizes with different text lengths
    for &text in &[SMALL_TEXT, MEDIUM_TEXT, LARGE_TEXT] {
        for window_size in &[1, 2, 3, 5, 10] {
            benchmarks.push(bench_windowed_words(text, *window_size));
        }
    }

    // Additional benchmarks for edge cases
    benchmarks.extend([
        // Empty string
        benchmark_fn("windowed_words/empty_string", |b| {
            let mut vec = VecDeque::new();
            b.iter(move || {
                let iter = "".windowed_words_in(3, &mut vec);

                for elem in iter {
                    black_box(elem);
                }
            })
        }),
        // Single word
        benchmark_fn("windowed_words/single_word", |b| {
            let mut vec = VecDeque::new();
            b.iter(move || {
                let iter = "Word".windowed_words_in(3, &mut vec);

                for elem in iter {
                    black_box(elem);
                }
            })
        }),
        // UTF-8 text
        benchmark_fn("windowed_words/utf8_text", |b| {
            let mut vec = VecDeque::new();
            b.iter(move || {
                let iter = "Hello 世界 Rust язык 🌍 Programming".windowed_words_in(3, &mut vec);

                for elem in iter {
                    black_box(elem);
                }
            })
        }),
    ]);

    benchmarks
}

// Customized settings to reduce variability
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
