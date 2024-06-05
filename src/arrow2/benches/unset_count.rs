use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::bitmap::utils::count_zeros;

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let bytes = (0..size as u32)
            .map(|x| 0b01011011u8.rotate_left(x))
            .collect::<Vec<_>>();

        c.bench_function(&format!("count_zeros 2^{log2_size}"), |b| {
            b.iter(|| count_zeros(&bytes, 0, bytes.len() * 8))
        });

        c.bench_function(&format!("count_zeros offset 2^{log2_size}"), |b| {
            b.iter(|| count_zeros(&bytes, 10, bytes.len() * 8 - 10))
        });
    })
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
