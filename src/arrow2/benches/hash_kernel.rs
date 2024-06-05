use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::compute::hash::hash;
use arrow2::util::bench_util::*;

fn add_benchmark(c: &mut Criterion) {
    let log2_size = 10;
    let size = 2usize.pow(log2_size);

    let arr_a = create_primitive_array::<i32>(size, 0.0);

    c.bench_function(&format!("i32 2^{log2_size}"), |b| b.iter(|| hash(&arr_a)));

    let arr_a = create_primitive_array::<i64>(size, 0.0);

    c.bench_function(&format!("i64 2^{log2_size}"), |b| b.iter(|| hash(&arr_a)));

    let arr_a = create_string_array::<i32>(size, 5, 0.0, 0);

    c.bench_function(&format!("str 2^{log2_size}"), |b| b.iter(|| hash(&arr_a)));

    let arr_a = create_boolean_array(size, 0.5, 0.0);

    c.bench_function(&format!("bool 2^{log2_size}"), |b| b.iter(|| hash(&arr_a)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
