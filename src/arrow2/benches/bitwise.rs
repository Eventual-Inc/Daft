use std::ops::{BitAnd, BitOr, BitXor, Not};

use criterion::{criterion_group, criterion_main, Criterion};
use num_traits::NumCast;

use arrow2::{
    array::PrimitiveArray, compute::bitwise::*, types::NativeType,
    util::bench_util::create_primitive_array_with_seed,
};

fn bench_or<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>)
where
    T: NativeType + BitOr<Output = T> + NumCast,
{
    criterion::black_box(or(lhs, rhs));
}

fn bench_xor<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>)
where
    T: NativeType + BitXor<Output = T> + NumCast,
{
    criterion::black_box(xor(lhs, rhs));
}

fn bench_and<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>)
where
    T: NativeType + BitAnd<Output = T> + NumCast,
{
    criterion::black_box(and(lhs, rhs));
}

fn bench_not<T>(arr: &PrimitiveArray<T>)
where
    T: NativeType + Not<Output = T> + NumCast,
{
    criterion::black_box(not(arr));
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let arr_a = create_primitive_array_with_seed::<u64>(size, 0.0, 43);
        let arr_b = create_primitive_array_with_seed::<u64>(size, 0.0, 42);

        c.bench_function(&format!("or 2^{log2_size}"), |b| {
            b.iter(|| bench_or(&arr_a, &arr_b))
        });

        c.bench_function(&format!("xor 2^{log2_size}"), |b| {
            b.iter(|| bench_xor(&arr_a, &arr_b))
        });

        c.bench_function(&format!("and 2^{log2_size}"), |b| {
            b.iter(|| bench_and(&arr_a, &arr_b))
        });

        c.bench_function(&format!("not 2^{log2_size}"), |b| {
            b.iter(|| bench_not(&arr_a))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
