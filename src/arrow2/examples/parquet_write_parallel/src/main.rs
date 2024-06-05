//! Example demonstrating how to write to parquet in parallel.
use std::collections::VecDeque;

use rayon::prelude::*;

use arrow2::{
    array::*,
    chunk::Chunk as AChunk,
    datatypes::*,
    error::{Error, Result},
    io::parquet::{read::ParquetError, write::*},
};

type Chunk = AChunk<Box<dyn Array>>;

struct Bla {
    columns: VecDeque<CompressedPage>,
    current: Option<CompressedPage>,
}

impl Bla {
    pub fn new(columns: VecDeque<CompressedPage>) -> Self {
        Self {
            columns,
            current: None,
        }
    }
}

impl FallibleStreamingIterator for Bla {
    type Item = CompressedPage;
    type Error = Error;

    fn advance(&mut self) -> Result<()> {
        self.current = self.columns.pop_front();
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}

fn parallel_write(path: &str, schema: Schema, chunks: &[Chunk]) -> Result<()> {
    // declare the options
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Snappy,
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let encoding_map = |data_type: &DataType| {
        match data_type.to_physical_type() {
            // remaining is plain
            _ => Encoding::Plain,
        }
    };

    // declare encodings
    let encodings = (&schema.fields)
        .iter()
        .map(|f| transverse(&f.data_type, encoding_map))
        .collect::<Vec<_>>();

    // derive the parquet schema (physical types) from arrow's schema.
    let parquet_schema = to_parquet_schema(&schema)?;

    let row_groups = chunks.iter().map(|chunk| {
        // write batch to pages; parallelized by rayon
        let columns = chunk
            .columns()
            .par_iter()
            .zip(parquet_schema.fields().to_vec())
            .zip(encodings.par_iter())
            .flat_map(move |((array, type_), encoding)| {
                let encoded_columns = array_to_columns(array, type_, options, encoding).unwrap();
                encoded_columns
                    .into_iter()
                    .map(|encoded_pages| {
                        let encoded_pages = DynIter::new(
                            encoded_pages
                                .into_iter()
                                .map(|x| x.map_err(|e| ParquetError::General(e.to_string()))),
                        );
                        encoded_pages
                            .map(|page| {
                                compress(page?, vec![], options.compression).map_err(|x| x.into())
                            })
                            .collect::<Result<VecDeque<_>>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Result<Vec<VecDeque<CompressedPage>>>>()?;

        let row_group = DynIter::new(
            columns
                .into_iter()
                .map(|column| Ok(DynStreamingIterator::new(Bla::new(column)))),
        );
        Result::Ok(row_group)
    });

    // Create a new empty file
    let file = std::io::BufWriter::new(std::fs::File::create(path)?);

    let mut writer = FileWriter::try_new(file, schema, options)?;

    // Write the file.
    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;

    Ok(())
}

fn create_chunk(size: usize) -> Result<Chunk> {
    let c1: Int32Array = (0..size)
        .map(|x| if x % 9 == 0 { None } else { Some(x as i32) })
        .collect();
    let c2: Utf8Array<i64> = (0..size)
        .map(|x| {
            if x % 8 == 0 {
                None
            } else {
                Some(x.to_string())
            }
        })
        .collect();

    Chunk::try_new(vec![
        c1.clone().boxed(),
        c1.clone().boxed(),
        c1.boxed(),
        c2.boxed(),
    ])
}

fn main() -> Result<()> {
    let fields = vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Int32, true),
        Field::new("c4", DataType::LargeUtf8, true),
    ];
    let chunk = create_chunk(100_000_000)?;

    let start = std::time::SystemTime::now();
    parallel_write("example.parquet", fields.into(), &[chunk])?;
    println!("took: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())
}
