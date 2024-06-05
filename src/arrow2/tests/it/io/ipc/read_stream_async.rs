use futures::StreamExt;
use tokio::fs::File;
use tokio_util::compat::*;

use arrow2::error::Result;
use arrow2::io::ipc::read::stream_async::*;

use crate::io::ipc::common::read_gzip_json;

async fn test_file(version: &str, file_name: &str) -> Result<()> {
    let testdata = crate::test_util::arrow_test_data();
    let mut file = File::open(format!(
        "{testdata}/arrow-ipc-stream/integration/{version}/{file_name}.stream"
    ))
    .await?
    .compat();

    let metadata = read_stream_metadata_async(&mut file).await?;
    let mut reader = AsyncStreamReader::new(file, metadata);

    // read expected JSON output
    let (schema, ipc_fields, batches) = read_gzip_json(version, file_name)?;

    assert_eq!(&schema, &reader.metadata().schema);
    assert_eq!(&ipc_fields, &reader.metadata().ipc_schema.fields);

    let mut items = vec![];
    while let Some(item) = reader.next().await {
        items.push(item?)
    }

    batches
        .iter()
        .zip(items.into_iter())
        .for_each(|(lhs, rhs)| {
            assert_eq!(lhs, &rhs);
        });
    Ok(())
}

#[tokio::test]
async fn write_async() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive").await
}
