use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::{
    compute::concatenate::concatenate,
    util::bench_util::{create_boolean_array, create_primitive_array},
};

fn add_benchmark(c: &mut Criterion) {
    (20..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let array1 = create_primitive_array::<i32>(8, 0.5);
        let array2 = create_primitive_array::<i32>(size + 1, 0.5);

        c.bench_function(&format!("int32 concat aligned 2^{log2_size}"), |b| {
            b.iter(|| {
                let _ = concatenate(&[&array1, &array2]);
            })
        });

        let array1 = create_primitive_array::<i32>(9, 0.5);

        c.bench_function(&format!("int32 concat unaligned 2^{log2_size}"), |b| {
            b.iter(|| {
                let _ = concatenate(&[&array1, &array2]);
            })
        });

        let array1 = create_boolean_array(8, 0.5, 0.5);
        let array2 = create_boolean_array(size + 1, 0.5, 0.5);

        c.bench_function(&format!("boolean concat aligned 2^{log2_size}"), |b| {
            b.iter(|| {
                let _ = concatenate(&[&array1, &array2]);
            })
        });

        let array1 = create_boolean_array(9, 0.5, 0.5);

        c.bench_function(&format!("boolean concat unaligned 2^{log2_size}"), |b| {
            b.iter(|| {
                let _ = concatenate(&[&array1, &array2]);
            })
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
