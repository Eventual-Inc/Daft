use arrow2::util::bench_util::create_string_array;
use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::compute::like::like_utf8_scalar;

fn bench_like(array: &Utf8Array<i32>, pattern: &str) {
    criterion::black_box(like_utf8_scalar(array, pattern).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    for size_log2 in 16..21_u32 {
        let size = size_log2.pow(2) as usize;
        let array = create_string_array::<i32>(100, size, 0.0, 0);
        c.bench_function(&format!("LIKE length = 2^{}", size_log2), |b| {
            b.iter(|| bench_like(&array, "%abba%"))
        });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
