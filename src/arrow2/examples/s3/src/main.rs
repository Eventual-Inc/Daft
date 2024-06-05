use arrow2::array::{Array, Int64Array};
use arrow2::error::Result;
use arrow2::io::parquet::read;
use futures::future::BoxFuture;
use range_reader::{RangeOutput, RangedAsyncReader};
use s3::Bucket;

#[tokio::main]
async fn main() -> Result<()> {
    let bucket_name = "dev-jorgecardleitao";
    let region = "eu-central-1".parse().unwrap();
    let bucket = Bucket::new_public(bucket_name, region).unwrap();
    let path = "benches_65536.parquet".to_string();

    let (data, _) = bucket.head_object(&path).await.unwrap();
    let length = data.content_length.unwrap() as usize;
    println!("total size in bytes: {}", length);

    // this maps a range (start, length) to a s3 ranged-bytes request
    let range_get = Box::new(move |start: u64, length: usize| {
        let bucket = bucket.clone();
        let path = path.clone();
        Box::pin(async move {
            let bucket = bucket.clone();
            let path = path.clone();
            // to get a sense of what is being queried in s3
            println!("getting {} bytes starting at {}", length, start);
            let (mut data, _) = bucket
                // -1 because ranges are inclusive in `get_object_range`
                .get_object_range(&path, start, Some(start + length as u64 - 1))
                .await
                .map_err(|x| std::io::Error::new(std::io::ErrorKind::Other, x.to_string()))?;
            println!("got {}/{} bytes starting at {}", data.len(), length, start);
            data.truncate(length);
            Ok(RangeOutput { start, data })
        }) as BoxFuture<'static, std::io::Result<RangeOutput>>
    });

    // at least 4kb per s3 request. Adjust to your liking.
    let min_request_size = 4 * 1024;

    // this factory is used to create multiple http requests concurrently. Each
    // task will get its own `RangedAsyncReader`.
    let reader_factory = || {
        Box::pin(futures::future::ready(Ok(RangedAsyncReader::new(
            length,
            min_request_size,
            range_get.clone(),
        )))) as BoxFuture<'static, std::result::Result<RangedAsyncReader, std::io::Error>>
    };

    // we need one reader to read the files' metadata
    let mut reader = reader_factory().await?;

    let metadata = read::read_metadata_async(&mut reader).await?;

    let schema = read::infer_schema(&metadata)?;

    println!("total number of rows: {}", metadata.num_rows);
    println!("Infered Arrow schema: {:#?}", schema);

    // let's read the first row group only. Iterate over them to your liking
    let group = &metadata.row_groups[0];

    // no chunk size in deserializing
    let chunk_size = None;

    // this is IO-bounded (and issues a join, thus the reader_factory)
    let column_chunks =
        read::read_columns_many_async(reader_factory, group, schema.fields, chunk_size).await?;

    // this is CPU-bounded and should be sent to a separate thread-pool.
    // We do it here for simplicity
    let chunks = read::RowGroupDeserializer::new(column_chunks, group.num_rows(), None);
    let chunks = chunks.collect::<Result<Vec<_>>>()?;

    // this is a single chunk because chunk_size is `None`
    let chunk = &chunks[0];

    let array = chunk.arrays()[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // ... and have fun with it.
    println!("len: {}", array.len());
    println!("null_count: {}", array.null_count());
    Ok(())
}
