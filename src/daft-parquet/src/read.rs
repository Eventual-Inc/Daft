use std::{
    collections::{BTreeMap, HashSet},
    fmt::format,
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
    metadata::read_parquet_metadata,
    read_planner::{self, RangesContainer, ReadPlanBuilder},
};

fn plan_read_row_groups(
    uri: &str,
    columns: Option<&[&str]>,
    row_groups: Option<&[i64]>,
    metadata: &FileMetaData,
) -> DaftResult<ReadPlanBuilder> {
    let arrow_schema = infer_schema(metadata)?;
    let mut arrow_fields = arrow_schema.fields;
    if let Some(columns) = columns {
        let avail_names = arrow_fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name) {
                names_to_keep.insert(*col_name);
            } else {
                return Err(common_error::DaftError::FieldNotFound(format!(
                    "Field: {} not found in {:?} when planning read for parquet file",
                    col_name, avail_names
                )));
            }
        }

        arrow_fields.retain(|f| names_to_keep.contains(f.name.as_str()))
    };

    let num_row_groups = metadata.row_groups.len();
    let mut read_plan = read_planner::ReadPlanBuilder::new(uri);
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
        for field in arrow_fields.iter() {
            let field_name = field.name.clone();
            let filtered_cols = columns
                .iter()
                .filter(|x| x.descriptor().path_in_schema[0] == field_name)
                .collect::<Vec<_>>();

            for col in filtered_cols {
                let (start, len) = col.byte_range();
                let end = start + len;

                read_plan.add_range(start as usize, end as usize);
            }
        }
    }
    Ok(read_plan)
}

fn read_row_groups_from_ranges(
    reader: &RangesContainer,
    columns: Option<&[&str]>,
    row_groups: Option<&[i64]>,
    metadata: &FileMetaData,
) -> DaftResult<Table> {
    let arrow_schema = infer_schema(metadata)?;

    let mut arrow_fields = arrow_schema.fields;

    if let Some(columns) = columns {
        let avail_names = arrow_fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name) {
                names_to_keep.insert(*col_name);
            } else {
                return Err(common_error::DaftError::FieldNotFound(format!(
                    "Field: {} not found in {:?} when planning read for parquet file",
                    col_name, avail_names
                )));
            }
        }

        arrow_fields.retain(|f| names_to_keep.contains(f.name.as_str()))
    };
    let daft_schema = daft_core::schema::Schema::try_from(&arrow2::datatypes::Schema {
        fields: arrow_fields.clone(),
        metadata: BTreeMap::new(),
    })?;

    let mut daft_series = vec![vec![]; arrow_fields.len()];
    let num_row_groups = metadata.row_groups.len();

    let row_groups = match row_groups {
        Some(rg) => rg.to_vec(),
        None => (0i64..num_row_groups as i64).collect(),
    };

    for row_group in row_groups {
        if !(0i64..num_row_groups as i64).contains(&row_group) {
            panic!("out of row group index");
        }

        let rg = metadata.row_groups.get(row_group as usize).unwrap();

        let columns = rg.columns();
        for (ii, field) in arrow_fields.iter().enumerate() {
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

                // should stream this instead
                let range_reader = reader.get_range_reader(start as usize..end as usize)?;
                let pages = PageReader::new(
                    range_reader,
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

    use crate::{
        read::plan_read_row_groups,
        read::read_row_groups_from_ranges,
        read_planner::{CoalescePass, ReadPlanBuilder},
    };
    use std::io::Read;
    #[tokio::test]
    async fn test_parquet_read_planner() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/daft_tpch_100g_32part.parquet";

        let mut io_config = IOConfig::default();
        // io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);
        let size = io_client.single_url_get_size(file.into()).await?;
        let metadata =
            crate::metadata::read_parquet_metadata(file, size, io_client.clone()).await?;
        let mut plan = plan_read_row_groups(file, Some(&["L_ORDERKEY"]), Some(&[1, 2]), &metadata)?;

        plan.add_pass(Box::new(CoalescePass {
            max_hole_size: 1024 * 1024,
            max_request_size: 16 * 1024 * 1024,
        }));
        plan.run_passes()?;
        println!("{}", plan);
        let memory = plan.collect(io_client.clone()).await?;
        let table =
            read_row_groups_from_ranges(&memory, Some(&["L_ORDERKEY"]), Some(&[1, 2]), &metadata)?;
        println!("{}", table);
        Ok(())
    }
}
