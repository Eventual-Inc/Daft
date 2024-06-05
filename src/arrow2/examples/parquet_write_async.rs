use futures::SinkExt;
use tokio::fs::File;

use arrow2::{
    array::{Array, Int32Array},
    chunk::Chunk,
    datatypes::{Field, Schema},
    error::Result,
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileSink, Version, WriteOptions,
    },
};
use tokio_util::compat::TokioAsyncReadCompatExt;

async fn write_batch(path: &str, schema: Schema, columns: Chunk<Box<dyn Array>>) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let mut stream = futures::stream::iter(vec![Ok(columns)].into_iter());

    // Create a new empty file
    let file = File::create(path).await?.compat();

    let encodings = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
        .collect();

    let mut writer = FileSink::try_new(file, schema, encodings, options)?;

    writer.send_all(&mut stream).await?;
    writer.close().await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
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
    let columns = Chunk::new(vec![array.boxed()]);

    write_batch("test.parquet", schema, columns).await
}
