use std::iter::FromIterator;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::bitmap::*;

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let bitmap2 = Bitmap::from_iter((0..size).into_iter().map(|x| x % 3 == 0));

        c.bench_function(&format!("bitmap extend aligned 2^{log2_size}"), |b| {
            let mut bitmap1 = MutableBitmap::new();
            b.iter(|| {
                bitmap1.extend_from_bitmap(&bitmap2);
                bitmap1.clear();
            })
        });

        c.bench_function(&format!("bitmap extend unaligned 2^{log2_size}"), |b| {
            let mut bitmap1 = MutableBitmap::with_capacity(1);
            b.iter(|| {
                bitmap1.push(true);
                bitmap1.extend_from_bitmap(&bitmap2);
                bitmap1.clear();
            })
        });

        c.bench_function(
            &format!("bitmap extend_constant aligned 2^{log2_size}"),
            |b| {
                let mut bitmap1 = MutableBitmap::new();
                b.iter(|| {
                    bitmap1.extend_constant(size, true);
                    bitmap1.clear();
                })
            },
        );

        c.bench_function(
            &format!("bitmap extend_constant unaligned 2^{log2_size}"),
            |b| {
                let mut bitmap1 = MutableBitmap::with_capacity(1);
                b.iter(|| {
                    bitmap1.push(true);
                    bitmap1.extend_constant(size, true);
                    bitmap1.clear();
                })
            },
        );

        let iter = (0..size)
            .into_iter()
            .map(|x| x % 3 == 0)
            .collect::<Vec<_>>();
        c.bench_function(&format!("bitmap from_trusted_len 2^{log2_size}"), |b| {
            b.iter(|| {
                MutableBitmap::from_trusted_len_iter(iter.iter().copied());
            })
        });

        c.bench_function(
            &format!("bitmap extend_from_trusted_len_iter 2^{log2_size}"),
            |b| {
                b.iter(|| {
                    let mut a = MutableBitmap::from(&[true]);
                    a.extend_from_trusted_len_iter(iter.iter().copied());
                })
            },
        );
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
