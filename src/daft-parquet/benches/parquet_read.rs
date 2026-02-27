use std::{fs, path::PathBuf, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, StringArray, StringBuilder,
        StructArray,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::{properties::WriterProperties, reader::SerializedFileReader},
};
use tango_bench::{
    Benchmark, DEFAULT_SETTINGS, IntoBenchmarks, MeasurementSettings, benchmark_fn,
    tango_benchmarks, tango_main,
};

const NUM_ROWS: usize = 100_000;
const CHUNK_SIZE: usize = 10_000;

/// Directory for temporary benchmark parquet files.
/// Embeds NUM_ROWS and CHUNK_SIZE so param changes force regeneration.
fn bench_data_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("daft_parquet_bench_r{NUM_ROWS}_c{CHUNK_SIZE}"));
    fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// Test data generators
// ---------------------------------------------------------------------------

fn make_int64_array(n: usize, seed: u64) -> Int64Array {
    let mut rng = fastrand::Rng::with_seed(seed);
    Int64Array::from_iter_values((0..n).map(|_| rng.i64(..)))
}

fn make_float64_array(n: usize, seed: u64) -> Float64Array {
    let mut rng = fastrand::Rng::with_seed(seed);
    Float64Array::from_iter_values((0..n).map(|_| rng.f64()))
}

fn make_bool_array(n: usize, seed: u64) -> BooleanArray {
    let mut rng = fastrand::Rng::with_seed(seed);
    BooleanArray::from_iter((0..n).map(|_| Some(rng.bool())))
}

fn make_string_array(n: usize, seed: u64) -> StringArray {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut builder = StringBuilder::new();
    for _ in 0..n {
        let len = rng.usize(1..32);
        let s: String = (0..len).map(|_| rng.alphanumeric()).collect();
        builder.append_value(&s);
    }
    builder.finish()
}

fn make_dict_string_array(n: usize, seed: u64, cardinality: usize) -> StringArray {
    let mut rng = fastrand::Rng::with_seed(seed);
    let dict_values: Vec<String> = (0..cardinality).map(|i| format!("value_{i}")).collect();
    let mut builder = StringBuilder::new();
    for _ in 0..n {
        let idx = rng.usize(0..cardinality);
        builder.append_value(&dict_values[idx]);
    }
    builder.finish()
}

fn make_list_int64_array(n: usize, seed: u64) -> ListArray {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut offsets = vec![0i32];
    let mut values = vec![];
    for _ in 0..n {
        let list_len = rng.usize(1..10);
        for _ in 0..list_len {
            values.push(rng.i64(..));
        }
        offsets.push(values.len() as i32);
    }
    let values_array = Int64Array::from_iter_values(values);
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    ListArray::new(
        field,
        arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets)),
        Arc::new(values_array) as ArrayRef,
        None,
    )
}

fn make_struct_array(n: usize, seed: u64) -> StructArray {
    let int_col = Arc::new(make_int64_array(n, seed)) as ArrayRef;
    let str_col = Arc::new(make_string_array(n, seed + 1)) as ArrayRef;
    let fields = vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ];
    StructArray::new(fields.into(), vec![int_col, str_col], None)
}

// ---------------------------------------------------------------------------
// File generators (files are cached on disk between runs)
// ---------------------------------------------------------------------------

fn write_single_col_int64(compression: Compression) -> PathBuf {
    let suffix = match compression {
        Compression::SNAPPY => "snappy",
        Compression::ZSTD(_) => "zstd",
        Compression::UNCOMPRESSED => "uncompressed",
        _ => "other",
    };
    let path = bench_data_dir().join(format!("single_int64_{suffix}.parquet"));
    if path.exists() {
        return path;
    }
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(NUM_ROWS)
        .build();
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    for offset in (0..NUM_ROWS).step_by(CHUNK_SIZE) {
        let n = std::cmp::min(CHUNK_SIZE, NUM_ROWS - offset);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(make_int64_array(n, offset as u64))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn write_wide_table() -> PathBuf {
    let path = bench_data_dir().join("wide_20cols_mixed.parquet");
    if path.exists() {
        return path;
    }
    let mut fields = vec![];
    for i in 0..10 {
        fields.push(Field::new(format!("int_{i}"), DataType::Int64, false));
    }
    for i in 0..5 {
        fields.push(Field::new(format!("float_{i}"), DataType::Float64, false));
    }
    for i in 0..5 {
        fields.push(Field::new(format!("str_{i}"), DataType::Utf8, false));
    }
    let schema = Arc::new(Schema::new(fields));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(NUM_ROWS)
        .build();
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    for offset in (0..NUM_ROWS).step_by(CHUNK_SIZE) {
        let n = std::cmp::min(CHUNK_SIZE, NUM_ROWS - offset);
        let mut arrays: Vec<ArrayRef> = vec![];
        for i in 0..10 {
            arrays.push(Arc::new(make_int64_array(n, (offset + i) as u64)));
        }
        for i in 0..5 {
            arrays.push(Arc::new(make_float64_array(n, (offset + 10 + i) as u64)));
        }
        for i in 0..5 {
            arrays.push(Arc::new(make_string_array(n, (offset + 15 + i) as u64)));
        }
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn write_string_dict_encoded() -> PathBuf {
    let path = bench_data_dir().join("string_dict_encoded.parquet");
    if path.exists() {
        return path;
    }
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(NUM_ROWS)
        .build();
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    for offset in (0..NUM_ROWS).step_by(CHUNK_SIZE) {
        let n = std::cmp::min(CHUNK_SIZE, NUM_ROWS - offset);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(make_dict_string_array(n, offset as u64, 100))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn write_nested_list_of_int() -> PathBuf {
    let path = bench_data_dir().join("nested_list_of_int.parquet");
    if path.exists() {
        return path;
    }
    let list_field = Arc::new(Field::new("item", DataType::Int64, true));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        DataType::List(list_field),
        true,
    )]));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(NUM_ROWS)
        .build();
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    for offset in (0..NUM_ROWS).step_by(CHUNK_SIZE) {
        let n = std::cmp::min(CHUNK_SIZE, NUM_ROWS - offset);
        let list_arr = make_list_int64_array(n, offset as u64);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(list_arr) as ArrayRef]).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn write_nested_struct() -> PathBuf {
    let path = bench_data_dir().join("nested_struct.parquet");
    if path.exists() {
        return path;
    }
    let struct_fields = vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        DataType::Struct(struct_fields.into()),
        true,
    )]));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(NUM_ROWS)
        .build();
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    for offset in (0..NUM_ROWS).step_by(CHUNK_SIZE) {
        let n = std::cmp::min(CHUNK_SIZE, NUM_ROWS - offset);
        let struct_arr = make_struct_array(n, offset as u64);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_arr) as ArrayRef]).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

fn write_boolean_column() -> PathBuf {
    let path = bench_data_dir().join("boolean_col.parquet");
    if path.exists() {
        return path;
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        DataType::Boolean,
        false,
    )]));
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(NUM_ROWS)
        .build();
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    for offset in (0..NUM_ROWS).step_by(CHUNK_SIZE) {
        let n = std::cmp::min(CHUNK_SIZE, NUM_ROWS - offset);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(make_bool_array(n, offset as u64))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    path
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_metadata_parse() -> Benchmark {
    // Pre-generate the file.
    let path = write_single_col_int64(Compression::UNCOMPRESSED);
    let bytes = bytes::Bytes::from(fs::read(&path).unwrap());

    benchmark_fn("metadata_parse", move |b| {
        let bytes = bytes.clone();
        b.iter(move || SerializedFileReader::new(bytes.clone()).unwrap())
    })
}

fn bench_read_file(name: &'static str, path: PathBuf) -> Benchmark {
    benchmark_fn(name, move |b| {
        let path = path.clone();
        b.iter(move || {
            let file = fs::File::open(&path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .with_batch_size(8192)
                .build()
                .unwrap();
            let mut total_rows = 0usize;
            for batch in reader {
                total_rows += batch.unwrap().num_rows();
            }
            total_rows
        })
    })
}

fn all_benchmarks() -> impl IntoBenchmarks {
    // Pre-generate all files before benchmarking.
    let snappy = write_single_col_int64(Compression::SNAPPY);
    let zstd = write_single_col_int64(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()));
    let uncompressed = write_single_col_int64(Compression::UNCOMPRESSED);
    let wide = write_wide_table();
    let dict_str = write_string_dict_encoded();
    let list_int = write_nested_list_of_int();
    let nested_struct = write_nested_struct();
    let boolean = write_boolean_column();

    [
        bench_metadata_parse(),
        bench_read_file("single_col_int64_snappy", snappy),
        bench_read_file("single_col_int64_zstd", zstd),
        bench_read_file("single_col_int64_uncompressed", uncompressed),
        bench_read_file("wide_table_20cols_mixed", wide),
        bench_read_file("string_dict_encoded", dict_str),
        bench_read_file("nested_list_of_int", list_int),
        bench_read_file("nested_struct", nested_struct),
        bench_read_file("boolean_column", boolean),
    ]
}

const SETTINGS: MeasurementSettings = MeasurementSettings {
    min_iterations_per_sample: 3,
    cache_firewall: Some(64),
    yield_before_sample: true,
    randomize_stack: Some(4096),
    ..DEFAULT_SETTINGS
};

tango_benchmarks!(all_benchmarks());
tango_main!(SETTINGS);
