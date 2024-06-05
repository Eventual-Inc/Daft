use std::io::Read;
use std::{fs, io::Cursor, path::PathBuf};

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::error::Result;
use arrow2::io::parquet::read;

fn to_buffer(
    size: usize,
    nullable: bool,
    dict: bool,
    multi_page: bool,
    compressed: bool,
) -> Vec<u8> {
    let dir = env!("CARGO_MANIFEST_DIR");

    let dict = if dict { "dict/" } else { "" };
    let multi_page = if multi_page { "multi/" } else { "" };
    let compressed = if compressed { "snappy/" } else { "" };
    let nullable = if nullable { "" } else { "_required" };

    let path = PathBuf::from(dir).join(format!(
        "fixtures/pyarrow3/v1/{dict}{multi_page}{compressed}benches{nullable}_{size}.parquet",
    ));

    let metadata = fs::metadata(&path).expect("unable to read metadata");
    let mut file = fs::File::open(path).unwrap();
    let mut buffer = vec![0; metadata.len() as usize];
    file.read_exact(&mut buffer).expect("buffer overflow");
    buffer
}

fn read_chunk(buffer: &[u8], size: usize, column: usize) -> Result<()> {
    let mut reader = Cursor::new(buffer);

    let metadata = read::read_metadata(&mut reader)?;

    let schema = read::infer_schema(&metadata)?;

    let schema = schema.filter(|index, _| index == column);

    let reader = read::FileReader::new(reader, metadata.row_groups, schema, None, None, None);

    for maybe_chunk in reader {
        let columns = maybe_chunk?;
        assert_eq!(columns.len(), size);
    }
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let buffer = to_buffer(size, true, false, false, false);
        let a = format!("read i64 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 0).unwrap()));

        let buffer = to_buffer(size, true, true, false, false);
        let a = format!("read ts dict 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 11).unwrap()));

        let a = format!("read utf8 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 2).unwrap()));

        let a = format!("read utf8 large 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 6).unwrap()));

        let a = format!("read utf8 emoji 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 12).unwrap()));

        let a = format!("read bool 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 3).unwrap()));

        let buffer = to_buffer(size, true, true, false, false);
        let a = format!("read utf8 dict 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 2).unwrap()));

        let buffer = to_buffer(size, true, false, false, true);
        let a = format!("read i64 snappy 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 0).unwrap()));

        let buffer = to_buffer(size, true, false, true, false);
        let a = format!("read utf8 multi 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 2).unwrap()));

        let buffer = to_buffer(size, true, false, true, true);
        let a = format!("read utf8 multi snappy 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 2).unwrap()));

        let buffer = to_buffer(size, true, false, true, true);
        let a = format!("read i64 multi snappy 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 0).unwrap()));

        let buffer = to_buffer(size, false, false, false, false);
        let a = format!("read required utf8 2^{log2_size}");
        c.bench_function(&a, |b| b.iter(|| read_chunk(&buffer, size, 2).unwrap()));
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
