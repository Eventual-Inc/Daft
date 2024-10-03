use common_io_config::S3Config;

use crate::{object_io::ObjectSource, Result, S3LikeSource};

#[tokio::test]
async fn test_full_get_from_s3() -> Result<()> {
    let parquet_file_path = "s3://daft-public-data/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";
    let parquet_expected_md5 = "929674747af64a98aceaa6d895863bd3";

    let config = S3Config {
        anonymous: true,
        ..Default::default()
    };
    let client = S3LikeSource::get_client(&config).await?;
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

#[tokio::test]
async fn test_full_ls_from_s3() -> Result<()> {
    let file_path = "s3://daft-public-data/test_fixtures/parquet/";

    let config = S3Config {
        anonymous: true,
        ..Default::default()
    };
    let client = S3LikeSource::get_client(&config).await?;

    client.ls(file_path, true, None, None, None).await?;

    Ok(())
}
