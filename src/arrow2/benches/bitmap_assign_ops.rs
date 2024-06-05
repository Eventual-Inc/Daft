use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::bitmap::{binary_assign, unary_assign};
use arrow2::bitmap::{Bitmap, MutableBitmap};

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let mut bitmap: MutableBitmap = (0..size).into_iter().map(|x| x % 3 == 0).collect();
        c.bench_function(&format!("mutablebitmap not 2^{log2_size}"), |b| {
            b.iter(|| {
                unary_assign(criterion::black_box(&mut bitmap), |x: u64| !x);
                assert!(!bitmap.is_empty());
            })
        });

        let bitmap: Bitmap = (0..size).into_iter().map(|x| x % 3 == 0).collect();
        c.bench_function(&format!("bitmap not 2^{log2_size}"), |b| {
            b.iter(|| {
                let r = !criterion::black_box(&bitmap);
                assert!(!r.is_empty());
            })
        });

        let mut lhs: MutableBitmap = (0..size).into_iter().map(|x| x % 3 == 0).collect();
        let rhs: Bitmap = (0..size).into_iter().map(|x| x % 4 == 0).collect();
        c.bench_function(&format!("mutablebitmap and 2^{log2_size}"), |b| {
            b.iter(|| {
                binary_assign(criterion::black_box(&mut lhs), &rhs, |x: u64, y| x & y);
                assert!(!bitmap.is_empty());
            })
        });

        let lhs: Bitmap = (0..size).into_iter().map(|x| x % 3 == 0).collect();
        let rhs: Bitmap = (0..size).into_iter().map(|x| x % 4 == 0).collect();
        c.bench_function(&format!("bitmap and 2^{log2_size}"), |b| {
            b.iter(|| {
                let r = criterion::black_box(&lhs) & &rhs;
                assert!(!r.is_empty());
            })
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
