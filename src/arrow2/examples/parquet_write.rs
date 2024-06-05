use std::fs::File;

use arrow2::{
    array::{Array, Int32Array},
    chunk::Chunk,
    datatypes::{Field, Schema},
    error::Result,
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};

fn write_chunk(path: &str, schema: Schema, chunk: Chunk<Box<dyn Array>>) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let iter = vec![Ok(chunk)];

    let encodings = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
        .collect();

    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings)?;

    // Create a new empty file
    let file = File::create(path)?;

    let mut writer = FileWriter::try_new(file, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;
    Ok(())
}

fn main() -> Result<()> {
    let array = Int32Array::from(&[
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]);
    let field = Field::new("c1", array.data_type().clone(), true);
    let schema = Schema::from(vec![field]);
    let chunk = Chunk::new(vec![array.boxed()]);

    write_chunk("test.parquet", schema, chunk)
}
