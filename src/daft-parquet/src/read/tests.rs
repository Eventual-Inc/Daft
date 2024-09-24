use std::sync::Arc;

use common_error::DaftResult;
use daft_io::{IOClient, IOConfig};
use futures::StreamExt;
use parquet2::metadata::FileMetaData;

use super::{read_parquet, read_parquet_metadata, stream_parquet};

const PARQUET_FILE: &str = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
const PARQUET_FILE_LOCAL: &str = "tests/assets/parquet-data/mvp.parquet";

fn get_local_parquet_path() -> String {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("../../"); // CARGO_MANIFEST_DIR is at src/daft-parquet
    d.push(PARQUET_FILE_LOCAL);
    d.to_str().unwrap().to_string()
}

#[test]
fn test_parquet_read_from_s3() -> DaftResult<()> {
    let file = PARQUET_FILE;

    let mut io_config = IOConfig::default();
    io_config.s3.anonymous = true;

    let io_client = Arc::new(IOClient::new(io_config.into())?);

    let table = read_parquet(
        file,
        None,
        None,
        None,
        None,
        None,
        io_client,
        None,
        true,
        Default::default(),
        None,
    )?;
    assert_eq!(table.len(), 100);

    Ok(())
}

#[test]
fn test_parquet_streaming_read_from_s3() -> DaftResult<()> {
    let file = PARQUET_FILE;

    let mut io_config = IOConfig::default();
    io_config.s3.anonymous = true;

    let io_client = Arc::new(IOClient::new(io_config.into())?);
    let runtime_handle = daft_io::get_runtime(true)?;
    runtime_handle.block_on_current_thread(async move {
        let tables = stream_parquet(
            file,
            None,
            None,
            None,
            None,
            None,
            io_client,
            None,
            &Default::default(),
            None,
            None,
            false,
        )
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<DaftResult<Vec<_>>>()?;
        let total_tables_len = tables.iter().map(|t| t.len()).sum::<usize>();
        assert_eq!(total_tables_len, 100);
        Ok(())
    })
}

#[test]
fn test_file_metadata_serialize_roundtrip() -> DaftResult<()> {
    let file = get_local_parquet_path();

    let io_config = IOConfig::default();
    let io_client = Arc::new(IOClient::new(io_config.into())?);
    let runtime_handle = daft_io::get_runtime(true)?;

    runtime_handle.block_on_io_pool(async move {
        let metadata = read_parquet_metadata(&file, io_client, None, None).await?;
        let serialized = bincode::serialize(&metadata).unwrap();
        let deserialized = bincode::deserialize::<FileMetaData>(&serialized).unwrap();
        assert_eq!(metadata, deserialized);
        Ok(())
    })?
}
