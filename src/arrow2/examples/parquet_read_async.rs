use std::time::SystemTime;

use futures::future::BoxFuture;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::compat::*;

use arrow2::error::Result;
use arrow2::io::parquet::read::{self, RowGroupDeserializer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let start = SystemTime::now();

    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = Box::new(args[1].clone());

    // # Read metadata
    let mut reader = BufReader::new(File::open(file_path.as_ref()).await?).compat();

    // this operation is usually done before reading the data, during planning.
    // This is a mix of IO and CPU-bounded tasks but both of them are O(1)
    let metadata = read::read_metadata_async(&mut reader).await?;
    let schema = read::infer_schema(&metadata)?;

    // This factory yields one file descriptor per column and is used to read columns concurrently.
    // They do not need to be buffered since we execute exactly 1 seek and 1 read on them.
    let factory = || {
        Box::pin(async { Ok(File::open(file_path.clone().as_ref()).await?.compat()) })
            as BoxFuture<_>
    };

    // This is the row group loop. Groups can be skipped based on the statistics they carry.
    for row_group in &metadata.row_groups {
        // A row group is consumed in two steps: the first step is to read the (compressed)
        // columns into memory, which is IO-bounded.
        let column_chunks = read::read_columns_many_async(
            factory,
            row_group,
            schema.fields.clone(),
            None,
            None,
            None,
        )
        .await?;

        // the second step is to iterate over the columns in chunks.
        // this operation is CPU-bounded and should be sent to a separate thread pool (e.g. `tokio_rayon`) to not block
        // the runtime.
        // Furthermore, this operation is trivially paralellizable e.g. via rayon, as each iterator
        // can be advanced in parallel (parallel decompression and deserialization).
        let chunks = RowGroupDeserializer::new(column_chunks, row_group.num_rows(), None);
        for maybe_chunk in chunks {
            let chunk = maybe_chunk?;
            println!("{}", chunk.len());
        }
    }
    println!("took: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())
}
