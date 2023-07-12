use std::sync::Arc;

use arrow2::io::parquet::read::{column_iter_to_arrays, infer_schema};
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::{get_runtime, IOClient};
use daft_table::Table;
use parquet2::{
    metadata::FileMetaData,
    read::{BasicDecompressor, PageReader},
};

use crate::metadata::read_parquet_metadata;

async fn read_row_groups(
    uri: &str,
    row_groups: Option<&[i64]>,
    metadata: &FileMetaData,
    io_client: Arc<IOClient>,
) -> DaftResult<Table> {
    let arrow_schema = infer_schema(metadata)?;
    let daft_schema = daft_core::schema::Schema::try_from(&arrow_schema)?;
    let mut daft_series = vec![vec![]; daft_schema.names().len()];
    let num_row_groups = metadata.row_groups.len();

    let row_groups = match row_groups {
        Some(rg) => rg.to_vec(),
        None => (0i64..num_row_groups as i64).collect(),
    };

    for row_group in row_groups {
        if !(0i64..num_row_groups as i64).contains(&row_group) {
            return Err(super::Error::ParquetRowGroupOutOfIndex {
                path: uri.into(),
                row_group,
                total_row_groups: num_row_groups as i64,
            }
            .into());
        }

        let rg = metadata.row_groups.get(row_group as usize).unwrap();

        let columns = rg.columns();
        for (ii, field) in arrow_schema.fields.iter().enumerate() {
            let field_name = field.name.clone();
            let mut decompressed_iters = vec![];
            let mut ptypes = vec![];
            let filtered_cols = columns
                .iter()
                .filter(|x| x.descriptor().path_in_schema[0] == field_name)
                .collect::<Vec<_>>();

            for col in filtered_cols {
                let (start, len) = col.byte_range();
                let end = start + len;

                // should be async
                let get_result = io_client
                    .single_url_get(uri.into(), Some(start as usize..end as usize))
                    .await?;

                // should stream this instead
                let bytes = get_result.bytes().await?;
                let buffer = bytes.to_vec();
                let pages = PageReader::new(
                    std::io::Cursor::new(buffer),
                    col,
                    Arc::new(|_, _| true),
                    vec![],
                    4 * 1024 * 1024,
                );

                decompressed_iters.push(BasicDecompressor::new(pages, vec![]));

                ptypes.push(&col.descriptor().descriptor.primitive_type);
            }

            // let field = &arrow_schema.fields[ii];
            let arr_iter = column_iter_to_arrays(
                decompressed_iters,
                ptypes,
                field.clone(),
                Some(4096),
                rg.num_rows(),
            )?;

            let all_arrays = arr_iter.collect::<arrow2::error::Result<Vec<_>>>()?;
            let ser = all_arrays
                .into_iter()
                .map(|a| Series::try_from((field.name.as_str(), cast_array_for_daft_if_needed(a))))
                .collect::<DaftResult<Vec<Series>>>()?;

            daft_series[ii].extend(ser);
        }
    }

    let compacted_series = daft_series
        .into_iter()
        .map(|s| Series::concat(s.iter().collect::<Vec<_>>().as_ref()))
        .collect::<DaftResult<_>>()?;

    Table::new(daft_schema, compacted_series)
}

pub fn read_parquet(
    uri: &str,
    row_groups: Option<&[i64]>,
    size: Option<usize>,
    io_client: Arc<IOClient>,
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();

    runtime_handle.block_on(async {
        let size = match size {
            Some(size) => size,
            None => io_client.single_url_get_size(uri.into()).await?,
        };
        let metadata = read_parquet_metadata(uri, size, io_client.clone()).await?;
        read_row_groups(uri, row_groups, &metadata, io_client.clone()).await
    })
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

        let table = read_parquet(file, None, None, io_client)?;
        assert_eq!(table.len(), 100);

        Ok(())
    }
}
