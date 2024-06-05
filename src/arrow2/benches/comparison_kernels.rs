use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::compute::comparison::{eq, eq_scalar};
use arrow2::scalar::*;
use arrow2::util::bench_util::*;

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let arr_a = create_primitive_array_with_seed::<f32>(size, 0.0, 42);
        let arr_b = create_primitive_array_with_seed::<f32>(size, 0.0, 43);

        c.bench_function(&format!("f32 2^{log2_size}"), |b| {
            b.iter(|| eq(&arr_a, &arr_b))
        });
        c.bench_function(&format!("f32 scalar 2^{log2_size}"), |b| {
            b.iter(|| eq_scalar(&arr_a, &PrimitiveScalar::<f32>::from(Some(0.5))))
        });

        let arr_a = create_boolean_array(size, 0.0, 0.1);
        let arr_b = create_boolean_array(size, 0.0, 0.2);

        c.bench_function(&format!("bool 2^{log2_size}"), |b| {
            b.iter(|| eq(&arr_a, &arr_b))
        });
        c.bench_function(&format!("bool scalar 2^{log2_size}"), |b| {
            b.iter(|| eq_scalar(&arr_a, &BooleanScalar::from(Some(false))))
        });

        let arr_a = create_string_array::<i32>(size, 4, 0.1, 42);
        let arr_b = create_string_array::<i32>(size, 4, 0.1, 43);
        c.bench_function(&format!("utf8 2^{log2_size}"), |b| {
            b.iter(|| eq(&arr_a, &arr_b))
        });

        c.bench_function(&format!("utf8 2^{log2_size}"), |b| {
            b.iter(|| eq_scalar(&arr_a, &Utf8Scalar::<i32>::from(Some("abc"))))
        });
    })
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
