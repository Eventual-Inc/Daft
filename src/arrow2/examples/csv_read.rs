use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::error::Result;
use arrow2::io::csv::read;

fn read_path(path: &str, projection: Option<&[usize]>) -> Result<Chunk<Box<dyn Array>>> {
    // Create a CSV reader. This is typically created on the thread that reads the file and
    // thus owns the read head.
    let mut reader = read::ReaderBuilder::new().from_path(path)?;

    // Infers the fields using the default inferer. The inferer is just a function that maps bytes
    // to a `DataType`.
    let (fields, _) = read::infer_schema(&mut reader, None, true, &read::infer)?;

    // allocate space to read from CSV to. The size of this vec denotes how many rows are read.
    let mut rows = vec![read::ByteRecord::default(); 100];

    // skip 0 (excluding the header) and read up to 100 rows.
    // this is IO-intensive and performs minimal CPU work. In particular,
    // no deserialization is performed.
    let rows_read = read::read_rows(&mut reader, 0, &mut rows)?;
    let rows = &rows[..rows_read];

    // parse the rows into a `Chunk`. This is CPU-intensive, has no IO,
    // and can be performed on a different thread by passing `rows` through a channel.
    // `deserialize_column` is a function that maps rows and a column index to an Array
    read::deserialize_batch(rows, &fields, projection, 0, read::deserialize_column)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let batch = read_path(file_path, None)?;
    println!("{batch:?}");
    Ok(())
}
