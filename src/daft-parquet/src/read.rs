use std::sync::Arc;

use arrow2::io::parquet::read::{column_iter_to_arrays, infer_schema};
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::{config::IOConfig, get_io_client, get_runtime, IOClient};
use daft_table::Table;
use parquet2::{
    metadata::FileMetaData,
    read::{BasicDecompressor, PageReader},
};

use crate::metadata::read_parquet_metadata;

async fn read_row_groups(
    uri: &str,
    row_groups: &[usize],
    metadata: &FileMetaData,
    io_client: Arc<IOClient>,
) -> DaftResult<Table> {
    let arrow_schema = infer_schema(metadata).unwrap();
    let daft_schema = daft_core::schema::Schema::try_from(&arrow_schema).unwrap();
    let mut daft_series = vec![vec![]; daft_schema.names().len()];

    for row_group in row_groups {
        let rg = metadata.row_groups.get(*row_group).unwrap();

        let columns = rg.columns();

        for (ii, col) in columns.iter().enumerate() {
            let (start, len) = col.byte_range();
            let end = start + len;

            // should be async
            let get_result = io_client
                .single_url_get(uri.into(), Some(start as usize..end as usize))
                .await
                .unwrap();

            // // should stream this instead
            let bytes = get_result.bytes().await.unwrap();
            let buffer = bytes.to_vec();
            let pages = PageReader::new(
                std::io::Cursor::new(buffer),
                col,
                Arc::new(|_, _| true),
                vec![],
                4 * 1024 * 1024,
            );

            let decom = BasicDecompressor::new(pages, vec![]);
            let ptype = &col.descriptor().descriptor.primitive_type;

            let field = &arrow_schema.fields[ii];
            let arr_iter = column_iter_to_arrays(
                vec![decom],
                vec![ptype],
                field.clone(),
                None,
                col.num_values() as usize,
            )
            .unwrap();
            let all_arrays = arr_iter.collect::<arrow2::error::Result<Vec<_>>>().unwrap();
            let ser = all_arrays
                .into_iter()
                .map(|a| Series::try_from((field.name.as_str(), cast_array_for_daft_if_needed(a))))
                .collect::<DaftResult<Vec<Series>>>()
                .unwrap();

            // let series = ;
            daft_series[ii].extend(ser);
        }
    }

    let compacted_series = daft_series
        .into_iter()
        .map(|s| Series::concat(s.iter().collect::<Vec<_>>().as_ref()).unwrap())
        .collect();

    Table::new(daft_schema, compacted_series)
}

pub fn read_parquet(uri: &str, size: Option<usize>, io_config: Arc<IOConfig>) -> DaftResult<Table> {
    let runtime_handle = get_runtime(false)?;
    let _rt_guard = runtime_handle.enter();

    let io_client = get_io_client(io_config)?;
    let size = size.unwrap();

    runtime_handle.block_on(async {
        let metadata = read_parquet_metadata(uri, size, io_client.clone()).await?;
        let all_row_groups: Vec<_> = (0..metadata.row_groups.len()).collect();
        read_row_groups(uri, all_row_groups.as_slice(), &metadata, io_client.clone()).await
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_io::config::IOConfig;

    use super::read_parquet;
    #[test]
    fn test_parquet_read_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
        let size = 9882;
        let io_config = Arc::new(IOConfig::default());
        let table = read_parquet(file, Some(size), io_config)?;
        assert_eq!(table.len(), 100);

        Ok(())
    }
}
