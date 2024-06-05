//! Example demonstrating how to read from parquet in parallel using rayon
use std::fs::File;
use std::io::BufReader;
use std::time::SystemTime;

use log::trace;
use rayon::prelude::*;

use arrow2::{
    array::Array,
    chunk::Chunk,
    error::Result,
    io::parquet::read::{self, ArrayIter},
};

mod logger;

/// Advances each iterator in parallel
/// # Panic
/// If the iterators are empty
fn deserialize_parallel(iters: &mut [ArrayIter<'static>]) -> Result<Chunk<Box<dyn Array>>> {
    // CPU-bounded
    let arrays = iters
        .par_iter_mut()
        .map(|iter| iter.next().transpose())
        .collect::<Result<Vec<_>>>()?;

    Chunk::try_new(arrays.into_iter().map(|x| x.unwrap()).collect())
}

fn parallel_read(path: &str, row_group: usize) -> Result<()> {
    // open the file
    let mut file = BufReader::new(File::open(path)?);

    // read Parquet's metadata and infer Arrow schema
    let metadata = read::read_metadata(&mut file)?;
    let schema = read::infer_schema(&metadata)?;

    // select the row group from the metadata
    let row_group = &metadata.row_groups[row_group];

    let chunk_size = 1024 * 8 * 8;

    // read (IO-bounded) all columns into memory (use a subset of the fields to project)
    let mut columns =
        read::read_columns_many(&mut file, row_group, schema.fields, Some(chunk_size))?;

    // deserialize (CPU-bounded) to Arrow in chunks
    let mut num_rows = row_group.num_rows();
    while num_rows > 0 {
        num_rows = num_rows.saturating_sub(chunk_size);
        trace!("[parquet/deserialize][start]");
        let chunk = deserialize_parallel(&mut columns)?;
        trace!("[parquet/deserialize][end][{}]", chunk.len());
        assert!(!chunk.is_empty());
    }
    Ok(())
}

fn main() -> Result<()> {
    log::set_logger(&logger::LOGGER)
        .map(|()| log::set_max_level(log::LevelFilter::Trace))
        .unwrap();

    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let row_group = args[2].parse::<usize>().unwrap();

    let start = SystemTime::now();
    parallel_read(file_path, row_group)?;
    println!("took: {} ms", start.elapsed().unwrap().as_millis());

    Ok(())
}
