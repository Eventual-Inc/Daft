use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::compute::arity_assign::{binary, unary};
use arrow2::{
    compute::arithmetics::basic::{mul, mul_scalar},
    util::bench_util::*,
};

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let mut arr_a = create_primitive_array::<f32>(size, 0.2);
        c.bench_function(&format!("apply_mul 2^{log2_size}"), |b| {
            b.iter(|| {
                unary(criterion::black_box(&mut arr_a), |x| x * 1.01);
                assert!(!arr_a.value(10).is_nan());
            })
        });

        let arr_a = create_primitive_array::<f32>(size, 0.2);
        c.bench_function(&format!("mul 2^{log2_size}"), |b| {
            b.iter(|| {
                let a = mul_scalar(criterion::black_box(&arr_a), &1.01f32);
                assert!(!a.value(10).is_nan());
            })
        });

        let mut arr_a = create_primitive_array::<f32>(size, 0.2);
        let mut arr_b = create_primitive_array_with_seed::<f32>(size, 0.2, 10);
        // convert to be close to 1.01
        unary(&mut arr_b, |x| 1.01 + x / 20.0);

        c.bench_function(&format!("apply_mul null 2^{log2_size}"), |b| {
            b.iter(|| {
                binary(criterion::black_box(&mut arr_a), &arr_b, |x, y| x * y);
                assert!(!arr_a.value(10).is_nan());
            })
        });

        let arr_a = create_primitive_array::<f32>(size, 0.2);
        c.bench_function(&format!("mul null 2^{log2_size}"), |b| {
            b.iter(|| {
                let a = mul(criterion::black_box(&arr_a), &arr_b);
                assert!(!a.value(10).is_nan());
            })
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
