use std::sync::Arc;

use futures::pin_mut;
use futures::StreamExt;
use tokio::fs::File;
use tokio_util::compat::*;

use arrow2::error::Result;
use arrow2::io::avro::avro_schema::file::Block;
use arrow2::io::avro::avro_schema::read_async::{block_stream, decompress_block, read_metadata};
use arrow2::io::avro::read::{deserialize, infer_schema};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let mut reader = File::open(file_path).await?.compat();

    let metadata = read_metadata(&mut reader).await?;
    let schema = infer_schema(&metadata.record)?;
    let metadata = Arc::new(metadata);
    let projection = Arc::new(schema.fields.iter().map(|_| true).collect::<Vec<_>>());

    let blocks = block_stream(&mut reader, metadata.marker).await;

    pin_mut!(blocks);
    while let Some(mut block) = blocks.next().await.transpose()? {
        let schema = schema.clone();
        let metadata = metadata.clone();
        let projection = projection.clone();
        // the content here is CPU-bounded. It should run on a dedicated thread pool
        let handle = tokio::task::spawn_blocking(move || {
            let mut decompressed = Block::new(0, vec![]);
            decompress_block(&mut block, &mut decompressed, metadata.compression)?;
            deserialize(
                &decompressed,
                &schema.fields,
                &metadata.record.fields,
                &projection,
            )
        });
        let chunk = handle.await.unwrap()?;
        assert!(!chunk.is_empty());
    }

    Ok(())
}
