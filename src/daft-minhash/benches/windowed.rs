use std::hint::black_box;

use daft_minhash::windowed::WindowedWords;
use tango_bench::{benchmark_fn, tango_benchmarks, tango_main, Benchmark, IntoBenchmarks};
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
            b.iter(move || {
                let iter = WindowedWords::new(black_box(text), black_box(window_size));
                // Force evaluation of the iterator
                let _result: Vec<_> = iter.collect();
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
            b.iter(|| {
                let iter = WindowedWords::new(black_box(""), black_box(3));
                let _result: Vec<_> = iter.collect();
            })
        }),
        // Single word
        benchmark_fn("windowed_words/single_word", |b| {
            b.iter(|| {
                let iter = WindowedWords::new(black_box("Word"), black_box(3));
                let _result: Vec<_> = iter.collect();
            })
        }),
        // UTF-8 text
        benchmark_fn("windowed_words/utf8_text", |b| {
            b.iter(|| {
                let iter = WindowedWords::new(
                    black_box("Hello 世界 Rust язык 🌍 Programming"),
                    black_box(3),
                );
                let _result: Vec<_> = iter.collect();
            })
        }),
    ]);

    benchmarks
}

tango_benchmarks!(all_benchmarks());
tango_main!();
