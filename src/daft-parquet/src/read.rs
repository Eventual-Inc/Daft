use std::sync::Arc;

use common_error::DaftResult;

use daft_io::{get_runtime, IOClient};
use daft_table::Table;

use crate::file::ParquetReaderBuilder;

pub fn read_parquet(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    io_client: Arc<IOClient>,
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    let (reader, ranges) = runtime_handle.block_on(async {
        let builder = ParquetReaderBuilder::from_uri(uri, io_client.clone()).await?;

        let builder = if let Some(columns) = columns {
            builder.prune_columns(columns)?
        } else {
            builder
        };
        let builder = builder.limit(start_offset, num_rows)?;
        let parquet_reader = builder.build()?;
        let ranges = parquet_reader.prebuffer_ranges(io_client.clone()).await?;
        DaftResult::Ok((parquet_reader, ranges))
    })?;

    reader.read_from_ranges(ranges)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_io::{config::IOConfig, IOClient};

    use super::read_parquet;
    #[test]
    fn test_parquet_read_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_parquet(file, None, None, None, io_client)?;
        assert_eq!(table.len(), 100);

        Ok(())
    }
}
