use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::compute::aggregate::*;
use arrow2::util::bench_util::*;

fn bench_sum(arr_a: &dyn Array) {
    sum(criterion::black_box(arr_a)).unwrap();
}

fn bench_min(arr_a: &dyn Array) {
    min(criterion::black_box(arr_a)).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let arr_a = create_primitive_array::<f32>(size, 0.0);

        c.bench_function(&format!("sum 2^{log2_size} f32"), |b| {
            b.iter(|| bench_sum(&arr_a))
        });
        c.bench_function(&format!("min 2^{log2_size} f32"), |b| {
            b.iter(|| bench_min(&arr_a))
        });

        let arr_a = create_primitive_array::<i32>(size, 0.0);

        c.bench_function(&format!("sum 2^{log2_size} i32"), |b| {
            b.iter(|| bench_sum(&arr_a))
        });
        c.bench_function(&format!("min 2^{log2_size} i32"), |b| {
            b.iter(|| bench_min(&arr_a))
        });

        let arr_a = create_primitive_array::<f32>(size, 0.1);

        c.bench_function(&format!("sum null 2^{log2_size} f32"), |b| {
            b.iter(|| bench_sum(&arr_a))
        });

        c.bench_function(&format!("min null 2^{log2_size} f32"), |b| {
            b.iter(|| bench_min(&arr_a))
        });

        let arr_a = create_string_array::<i32>(1, size, 0.0, 0);

        c.bench_function(&format!("min 2^{log2_size} utf8"), |b| {
            b.iter(|| bench_min(&arr_a))
        });

        let arr_a = create_string_array::<i32>(1, size, 0.1, 0);

        c.bench_function(&format!("min null 2^{log2_size} utf8"), |b| {
            b.iter(|| bench_min(&arr_a))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
