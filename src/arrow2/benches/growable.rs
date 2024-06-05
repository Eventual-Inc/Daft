use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::{
    array::growable::{Growable, GrowablePrimitive},
    util::bench_util::create_primitive_array,
};

fn add_benchmark(c: &mut Criterion) {
    let values = (0..1026).rev();

    let i32_array = create_primitive_array::<i32>(1026 * 10, 0.0);
    c.bench_function("growable::primitive::non_null::non_null", |b| {
        b.iter(|| {
            let mut a = GrowablePrimitive::new(vec![&i32_array], false, 1026 * 10);
            values
                .clone()
                .into_iter()
                .for_each(|start| a.extend(0, start, 10))
        })
    });

    let i32_array = create_primitive_array::<i32>(1026 * 10, 0.0);
    c.bench_function("growable::primitive::non_null::null", |b| {
        b.iter(|| {
            let mut a = GrowablePrimitive::new(vec![&i32_array], true, 1026 * 10);
            values.clone().into_iter().for_each(|start| {
                if start % 2 == 0 {
                    a.extend_validity(10);
                } else {
                    a.extend(0, start, 10)
                }
            })
        })
    });

    let i32_array = create_primitive_array::<i32>(1026 * 10, 0.1);

    let values = values.collect::<Vec<_>>();
    c.bench_function("growable::primitive::null::non_null", |b| {
        b.iter(|| {
            let mut a = GrowablePrimitive::new(vec![&i32_array], false, 1026 * 10);
            values
                .clone()
                .into_iter()
                .for_each(|start| a.extend(0, start, 10))
        })
    });
    c.bench_function("growable::primitive::null::null", |b| {
        b.iter(|| {
            let mut a = GrowablePrimitive::new(vec![&i32_array], true, 1026 * 10);
            values.clone().into_iter().for_each(|start| {
                if start % 2 == 0 {
                    a.extend_validity(10);
                } else {
                    a.extend(0, start, 10)
                }
            })
        })
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
