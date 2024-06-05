use rand::{rngs::StdRng, Rng, SeedableRng};

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::compute::take;
use arrow2::util::bench_util::*;

fn create_random_index(size: usize, null_density: f32) -> PrimitiveArray<i32> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen_range::<i32, _>(0i32..size as i32);
                Some(value)
            } else {
                None
            }
        })
        .collect::<PrimitiveArray<i32>>()
}

fn bench_take(values: &dyn Array, indices: &PrimitiveArray<i32>) {
    criterion::black_box(take::take(values, indices).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let values = create_primitive_array::<i32>(size, 0.0);
        let values_nulls = create_primitive_array::<i32>(size, 0.2);
        let indices = create_random_index(size, 0.0);
        let indices_nulls = create_random_index(size, 0.5);
        c.bench_function(&format!("take i32 2^{log2_size}"), |b| {
            b.iter(|| bench_take(&values, &indices))
        });

        c.bench_function(&format!("take i32 nulls 2^{log2_size}"), |b| {
            b.iter(|| bench_take(&values, &indices_nulls))
        });

        c.bench_function(&format!("take i32 values nulls 2^{log2_size}"), |b| {
            b.iter(|| bench_take(&values_nulls, &indices))
        });

        let values = create_boolean_array(size, 0.0, 0.5);
        c.bench_function(&format!("take bool 2^{log2_size}"), |b| {
            b.iter(|| bench_take(&values, &indices))
        });

        let values_nulls = create_boolean_array(size, 0.2, 0.5);
        c.bench_function(&format!("take bool values nulls 2^{log2_size}"), |b| {
            b.iter(|| bench_take(&values_nulls, &indices))
        });

        c.bench_function(&format!("take bool nulls 2^{log2_size}"), |b| {
            b.iter(|| bench_take(&values, &indices_nulls))
        });

        let values = create_string_array::<i32>(512, 4, 0.0, 42);
        c.bench_function(&format!("take str 2^{log2_size}"), |b| {
            b.iter(|| bench_take(&values, &indices))
        });

        c.bench_function(&format!("take str nulls 2^{log2_size}"), |b| {
            b.iter(|| bench_take(&values, &indices_nulls))
        });

        let values_nulls = create_string_array::<i32>(size, 4, 0.2, 42);
        c.bench_function(&format!("take str values nulls 2^{log2_size}"), |b| {
            b.iter(|| bench_take(&values_nulls, &indices))
        });
    });

    let values = create_string_array::<i32>(512, 4, 0.0, 42);
    let indices = create_random_index(512, 0.5);
    c.bench_function("take str null indices 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(1024, 4, 0.0, 42);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(1024, 4, 0.5, 42);

    let indices = create_random_index(1024, 0.0);
    c.bench_function("take str null values 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let values = create_string_array::<i32>(1024, 4, 0.5, 42);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take str null values null indices 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
