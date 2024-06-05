use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::bitmap::{utils::SlicesIterator, Bitmap};

fn bench_slices(lhs: &Bitmap) {
    let set_count = lhs.len() - lhs.unset_bits();
    let slices = SlicesIterator::new(lhs);

    let count = slices.fold(0usize, |acc, v| acc + v.1);
    assert_eq!(count, set_count);
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let bitmap: Bitmap = (0..size).into_iter().map(|x| x % 3 == 0).collect();
        c.bench_function(&format!("bitmap slices 2^{log2_size}"), |b| {
            b.iter(|| bench_slices(&bitmap))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
