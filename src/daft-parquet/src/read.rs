use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use arrow2::io::parquet::read::{column_iter_to_arrays, infer_schema};
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::{get_runtime, IOClient};
use daft_table::Table;
use parquet2::{
    metadata::FileMetaData,
    read::{BasicDecompressor, PageReader},
};

use crate::{
    file::ParquetReaderBuilder,
    metadata::read_parquet_metadata,
    read_planner::{self, CoalescePass, RangesContainer, ReadPlanner, SplitLargeRequestPass},
};

pub fn read_parquet(
    uri: &str,
    columns: Option<&[&str]>,
    row_groups: Option<&[i64]>,
    size: Option<usize>,
    io_client: Arc<IOClient>,
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    assert!(row_groups.is_none());
    let (reader, ranges) = runtime_handle.block_on(async {
        let builder = ParquetReaderBuilder::from_uri(uri, io_client.clone()).await?;

        let builder = if let Some(columns) = columns {
            builder.prune_columns(columns)?
        } else {
            builder
        };
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
    // #[test]
    // fn test_parquet_read_from_s3() -> DaftResult<()> {
    //     let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";

    //     let mut io_config = IOConfig::default();
    //     io_config.s3.anonymous = true;

    //     let io_client = Arc::new(IOClient::new(io_config.into())?);

    //     let table = read_parquet(file, None, None, None, io_client)?;
    //     assert_eq!(table.len(), 100);

    //     Ok(())
    // }

    // use crate::{
    //     read::plan_read_row_groups, read::read_row_groups_from_ranges, read_planner::CoalescePass,
    // };

    // #[tokio::test]
    // async fn test_parquet_read_planner() -> DaftResult<()> {
    //     let file = "s3://daft-public-data/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";

    //     let mut io_config = IOConfig::default();
    //     io_config.s3.anonymous = true;

    //     let io_client = Arc::new(IOClient::new(io_config.into())?);
    //     let size = io_client.single_url_get_size(file.into()).await?;
    //     let metadata =
    //         crate::metadata::read_parquet_metadata(file, size, io_client.clone()).await?;
    //     let mut plan = plan_read_row_groups(file, Some(&["P_PARTKEY"]), Some(&[1, 2]), &metadata)?;

    //     plan.add_pass(Box::new(CoalescePass {
    //         max_hole_size: 1024 * 1024,
    //         max_request_size: 16 * 1024 * 1024,
    //     }));
    //     plan.run_passes()?;
    //     let memory = plan.collect(io_client.clone()).await?;
    //     let _table =
    //         read_row_groups_from_ranges(&memory, Some(&["P_PARTKEY"]), Some(&[1, 2]), &metadata)?;
    //     Ok(())
    // }
}
