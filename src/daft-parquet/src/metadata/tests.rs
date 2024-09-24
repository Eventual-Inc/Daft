use std::sync::Arc;

use common_error::DaftResult;
use daft_io::{IOClient, IOConfig};

use super::read_parquet_metadata;

#[tokio::test]
async fn test_parquet_metadata_from_s3() -> DaftResult<()> {
    let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
    let size = 9882;

    let mut io_config = IOConfig::default();
    io_config.s3.anonymous = true;
    let io_client = Arc::new(IOClient::new(io_config.into())?);

    let metadata = read_parquet_metadata(file, size, io_client.clone(), None, None).await?;
    assert_eq!(metadata.num_rows, 100);

    Ok(())
}
