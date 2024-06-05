use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::bitmap::Bitmap;

fn bench_arrow2(lhs: &Bitmap, rhs: &Bitmap) {
    let r = lhs | rhs;
    assert!(r.unset_bits() > 0);
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let bitmap: Bitmap = (0..size).into_iter().map(|x| x % 3 == 0).collect();
        c.bench_function(&format!("bitmap aligned not 2^{log2_size}"), |b| {
            b.iter(|| {
                let r = !&bitmap;
                assert!(r.unset_bits() > 0);
            })
        });

        let offset = ((size as f64) * 0.1) as usize;
        let len = ((size as f64) * 0.85) as usize;

        c.bench_function(
            &format!("bitmap count zeros 85% slice 2^{log2_size}"),
            |b| {
                b.iter(|| {
                    let mut r = bitmap.clone();
                    r.slice(offset, len);
                    assert!(r.unset_bits() > 0);
                })
            },
        );

        let offset = ((size as f64) * 0.2) as usize;
        let len = ((size as f64) * 0.51) as usize;

        c.bench_function(
            &format!("bitmap count zeros 51% slice 2^{log2_size}"),
            |b| {
                b.iter(|| {
                    let mut r = bitmap.clone();
                    r.slice(offset, len);
                    assert!(r.unset_bits() > 0);
                })
            },
        );

        let mut bitmap1 = bitmap.clone();
        bitmap1.slice(1, size - 1);
        c.bench_function(&format!("bitmap not 2^{log2_size}"), |b| {
            b.iter(|| {
                let r = !&bitmap1;
                assert!(r.unset_bits() > 0);
            })
        });

        let bitmap1: Bitmap = (0..size).into_iter().map(|x| x % 4 == 0).collect();
        c.bench_function(&format!("bitmap aligned or 2^{log2_size}"), |b| {
            b.iter(|| bench_arrow2(&bitmap, &bitmap1))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
