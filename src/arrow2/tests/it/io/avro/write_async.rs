use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::avro::avro_schema::file::Compression;
use arrow2::io::avro::avro_schema::write_async::{write_block, write_metadata};
use arrow2::io::avro::write;

use super::read::read_avro;
use super::write::{data, schema, serialize_to_block};

async fn write_avro<R: AsRef<dyn Array>>(
    columns: &Chunk<R>,
    schema: &Schema,
    compression: Option<Compression>,
) -> Result<Vec<u8>> {
    // usually done on a different thread pool
    let compressed_block = serialize_to_block(columns, schema, compression)?;

    let record = write::to_record(schema)?;
    let mut file = vec![];

    write_metadata(&mut file, record, compression).await?;

    write_block(&mut file, &compressed_block).await?;

    Ok(file)
}

async fn roundtrip(compression: Option<Compression>) -> Result<()> {
    let expected = data();
    let expected_schema = schema();

    let data = write_avro(&expected, &expected_schema, compression).await?;

    let (result, read_schema) = read_avro(&data, None)?;

    assert_eq!(expected_schema, read_schema);
    for (c1, c2) in result.columns().iter().zip(expected.columns().iter()) {
        assert_eq!(c1.as_ref(), c2.as_ref());
    }
    Ok(())
}

#[tokio::test]
async fn no_compression() -> Result<()> {
    roundtrip(None).await
}
