use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::error::Result;
use arrow2::io::csv::write;
use arrow2::util::bench_util::*;

type ChunkBox = Chunk<Box<dyn Array>>;

fn write_batch(columns: &ChunkBox) -> Result<()> {
    let mut writer = vec![];

    assert_eq!(columns.arrays().len(), 1);
    let options = write::SerializeOptions::default();
    write::write_header(&mut writer, &["a"], &options)?;

    write::write_chunk(&mut writer, columns, &options)
}

fn make_chunk(array: impl Array + 'static) -> Chunk<Box<dyn Array>> {
    Chunk::new(vec![Box::new(array)])
}

fn add_benchmark(c: &mut Criterion) {
    (10..=18).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let array = create_primitive_array::<i32>(size, 0.1);
        let batch = make_chunk(array);

        c.bench_function(&format!("csv write i32 2^{log2_size}"), |b| {
            b.iter(|| write_batch(&batch))
        });

        let array = create_string_array::<i32>(size, 100, 0.1, 42);
        let batch = make_chunk(array);

        c.bench_function(&format!("csv write utf8 2^{log2_size}"), |b| {
            b.iter(|| write_batch(&batch))
        });

        let array = create_primitive_array::<f64>(size, 0.1);
        let batch = make_chunk(array);

        c.bench_function(&format!("csv write f64 2^{log2_size}"), |b| {
            b.iter(|| write_batch(&batch))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
