use arrow2::array::Array;
use arrow2::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::io::json::{read, write};
use arrow2::util::bench_util::*;

fn prep(array: impl Array + 'static) -> (Vec<u8>, DataType) {
    let mut data = vec![];
    let blocks = write::Serializer::new(
        vec![Ok(Box::new(array) as Box<dyn Array>)].into_iter(),
        vec![],
    );
    // the operation of writing is IO-bounded.
    write::write(&mut data, blocks).unwrap();

    let value = read::json_deserializer::parse(&data).unwrap();

    let dt = read::infer(&value).unwrap();
    (data, dt)
}

fn bench_read(data: &[u8], dt: &DataType) {
    let value = read::json_deserializer::parse(data).unwrap();
    read::deserialize(&value, dt.clone()).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let array = create_primitive_array::<i32>(size, 0.1);

        let (data, dt) = prep(array);

        c.bench_function(&format!("read i32 2^{log2_size}"), |b| {
            b.iter(|| bench_read(&data, &dt))
        });

        let array = create_primitive_array::<f64>(size, 0.1);

        let (data, dt) = prep(array);

        c.bench_function(&format!("read f64 2^{log2_size}"), |b| {
            b.iter(|| bench_read(&data, &dt))
        });

        let array = create_string_array::<i32>(size, 10, 0.1, 42);

        let (data, dt) = prep(array);

        c.bench_function(&format!("read utf8 2^{log2_size}"), |b| {
            b.iter(|| bench_read(&data, &dt))
        });

        let array = create_boolean_array(size, 0.1, 0.1);

        let (data, dt) = prep(array);

        c.bench_function(&format!("read bool 2^{log2_size}"), |b| {
            b.iter(|| bench_read(&data, &dt))
        });
    })
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
