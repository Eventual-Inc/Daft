use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::error::Error;
use arrow2::io::json::write;
use arrow2::util::bench_util::*;

fn write_array(array: Box<dyn Array>) -> Result<(), Error> {
    let mut writer = vec![];

    let arrays = vec![Ok(array)].into_iter();

    // Advancing this iterator serializes the next array to its internal buffer (i.e. CPU-bounded)
    let blocks = write::Serializer::new(arrays, vec![]);

    // the operation of writing is IO-bounded.
    write::write(&mut writer, blocks)?;

    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    (10..=18).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let array = create_primitive_array::<i32>(size, 0.1);

        c.bench_function(&format!("json write i32 2^{log2_size}"), |b| {
            b.iter(|| write_array(Box::new(array.clone())))
        });

        let array = create_string_array::<i32>(size, 100, 0.1, 42);

        c.bench_function(&format!("json write utf8 2^{log2_size}"), |b| {
            b.iter(|| write_array(Box::new(array.clone())))
        });

        let array = create_primitive_array::<f64>(size, 0.1);

        c.bench_function(&format!("json write f64 2^{log2_size}"), |b| {
            b.iter(|| write_array(Box::new(array.clone())))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
