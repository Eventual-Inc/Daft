use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::Utf8Array;

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let array = Utf8Array::<i32>::from_trusted_len_values_iter(
            std::iter::repeat("aaa")
                .take(size / 2)
                .chain(std::iter::repeat("bbb").take(size / 2)),
        );

        c.bench_function(&format!("iter_values 2^{log2_size}"), |b| {
            b.iter(|| {
                for x in array.values_iter() {
                    assert_eq!(x.len(), 3);
                }
            })
        });

        c.bench_function(&format!("iter 2^{log2_size}"), |b| {
            b.iter(|| {
                for x in array.iter() {
                    assert_eq!(x.unwrap().len(), 3);
                }
            })
        });
    })
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
