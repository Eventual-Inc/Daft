use std::default;

use crate::{object_io::ObjectSource, HttpSource, Result};

#[tokio::test]
async fn test_full_get_from_http() -> Result<()> {
    let parquet_file_path = "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";
    let parquet_expected_md5 = "929674747af64a98aceaa6d895863bd3";

    let client = HttpSource::get_client(&default::Default::default()).await?;
    let parquet_file = client.get(parquet_file_path, None, None).await?;
    let bytes = parquet_file.bytes().await?;
    let all_bytes = bytes.as_ref();
    let checksum = format!("{:x}", md5::compute(all_bytes));
    assert_eq!(checksum, parquet_expected_md5);

    let first_bytes = client
        .get(parquet_file_path, Some(0..10), None)
        .await?
        .bytes()
        .await?;
    assert_eq!(first_bytes.len(), 10);
    assert_eq!(first_bytes.as_ref(), &all_bytes[..10]);

    let first_bytes = client
        .get(parquet_file_path, Some(10..100), None)
        .await?
        .bytes()
        .await?;
    assert_eq!(first_bytes.len(), 90);
    assert_eq!(first_bytes.as_ref(), &all_bytes[10..100]);

    let last_bytes = client
        .get(
            parquet_file_path,
            Some((all_bytes.len() - 10)..(all_bytes.len() + 10)),
            None,
        )
        .await?
        .bytes()
        .await?;
    assert_eq!(last_bytes.len(), 10);
    assert_eq!(last_bytes.as_ref(), &all_bytes[(all_bytes.len() - 10)..]);

    let size_from_get_size = client.get_size(parquet_file_path, None).await?;
    assert_eq!(size_from_get_size, all_bytes.len());
    Ok(())
}
