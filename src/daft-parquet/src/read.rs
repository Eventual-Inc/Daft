use std::sync::Arc;

use common_error::DaftResult;

use daft_core::{
    datatypes::{Int32Array, TimeUnit, UInt64Array, Utf8Array},
    schema::Schema,
    DataType, IntoSeries, Series,
};
use daft_io::{get_runtime, IOClient};
use daft_table::Table;
use futures::future::{join_all, try_join_all};
use snafu::ResultExt;

use crate::{file::ParquetReaderBuilder, JoinSnafu};

#[derive(Clone)]
pub struct ParquetSchemaInferenceOptions {
    pub coerce_int96_timestamp_unit: TimeUnit,
}

impl ParquetSchemaInferenceOptions {
    pub fn new(coerce_int96_timestamp_unit: Option<TimeUnit>) -> Self {
        let default: ParquetSchemaInferenceOptions = Default::default();
        let coerce_int96_timestamp_unit =
            coerce_int96_timestamp_unit.unwrap_or(default.coerce_int96_timestamp_unit);
        ParquetSchemaInferenceOptions {
            coerce_int96_timestamp_unit,
        }
    }
}

impl Default for ParquetSchemaInferenceOptions {
    fn default() -> Self {
        ParquetSchemaInferenceOptions {
            coerce_int96_timestamp_unit: TimeUnit::Nanoseconds,
        }
    }
}

async fn read_parquet_single(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    io_client: Arc<IOClient>,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<Table> {
    let builder = ParquetReaderBuilder::from_uri(uri, io_client.clone()).await?;
    let builder = builder.set_infer_schema_options(schema_infer_options);

    let builder = if let Some(columns) = columns {
        builder.prune_columns(columns)?
    } else {
        builder
    };
    let builder = builder.limit(start_offset, num_rows)?;

    let metadata_num_rows = builder.metadata().num_rows;
    let metadata_num_columns = builder.parquet_schema().fields().len();

    let parquet_reader = builder.build()?;
    let ranges = parquet_reader.prebuffer_ranges(io_client)?;
    let table = parquet_reader.read_from_ranges(ranges).await?;

    match (start_offset, num_rows) {
        (None, None) if metadata_num_rows != table.len() => {
            Err(super::Error::ParquetNumRowMismatch {
                path: uri.into(),
                metadata_num_rows,
                read_rows: table.len(),
            })
        }
        (Some(s), None) if metadata_num_rows.saturating_sub(s) != table.len() => {
            Err(super::Error::ParquetNumRowMismatch {
                path: uri.into(),
                metadata_num_rows: metadata_num_rows.saturating_sub(s),
                read_rows: table.len(),
            })
        }
        (_, Some(n)) if n < table.len() => Err(super::Error::ParquetNumRowMismatch {
            path: uri.into(),
            metadata_num_rows: n.min(metadata_num_rows),
            read_rows: table.len(),
        }),
        _ => Ok(()),
    }?;

    let expected_num_columns = if let Some(columns) = columns {
        columns.len()
    } else {
        metadata_num_columns
    };

    if table.num_columns() != expected_num_columns {
        return Err(super::Error::ParquetNumColumnMismatch {
            path: uri.into(),
            metadata_num_columns: expected_num_columns,
            read_columns: table.num_columns(),
        }
        .into());
    }

    Ok(table)
}

pub fn read_parquet(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    io_client: Arc<IOClient>,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        read_parquet_single(
            uri,
            columns,
            start_offset,
            num_rows,
            io_client,
            schema_infer_options,
        )
        .await
    })
}

pub fn read_parquet_bulk(
    uris: &[&str],
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    io_client: Arc<IOClient>,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<Vec<Table>> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    let owned_columns = columns.map(|s| s.iter().map(|v| String::from(*v)).collect::<Vec<_>>());

    let tables = runtime_handle
        .block_on(async move {
            try_join_all(uris.iter().map(|uri| {
                let uri = uri.to_string();
                let owned_columns = owned_columns.clone();
                let io_client = io_client.clone();
                let schema_infer_options = schema_infer_options.clone();
                tokio::task::spawn(async move {
                    let columns = owned_columns
                        .as_ref()
                        .map(|s| s.iter().map(AsRef::as_ref).collect::<Vec<_>>());
                    read_parquet_single(
                        &uri,
                        columns.as_deref(),
                        start_offset,
                        num_rows,
                        io_client,
                        &schema_infer_options,
                    )
                    .await
                })
            }))
            .await
        })
        .context(JoinSnafu { path: "UNKNOWN" })?;
    tables.into_iter().collect::<DaftResult<Vec<_>>>()
}

pub fn read_parquet_schema(
    uri: &str,
    io_client: Arc<IOClient>,
    schema_inference_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<Schema> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    let builder = runtime_handle
        .block_on(async { ParquetReaderBuilder::from_uri(uri, io_client.clone()).await })?;
    let builder = builder.set_infer_schema_options(schema_inference_options);

    Schema::try_from(builder.build()?.arrow_schema())
}

pub fn read_parquet_statistics(uris: &Series, io_client: Arc<IOClient>) -> DaftResult<Table> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();

    if uris.data_type() != &DataType::Utf8 {
        return Err(common_error::DaftError::ValueError(format!(
            "Expected Utf8 Datatype, got {}",
            uris.data_type()
        )));
    }

    let path_array: &Utf8Array = uris.downcast()?;
    use daft_core::array::ops::as_arrow::AsArrow;
    let values = path_array.as_arrow();

    let handles_iter = values.iter().map(|uri| {
        let owned_string = uri.map(|v| v.to_string());
        let owned_client = io_client.clone();
        tokio::spawn(async move {
            if let Some(owned_string) = owned_string {
                let builder = ParquetReaderBuilder::from_uri(&owned_string, owned_client).await?;
                let num_rows = builder.metadata().num_rows;
                let num_row_groups = builder.metadata().row_groups.len();
                let version_num = builder.metadata().version;

                Ok((Some(num_rows), Some(num_row_groups), Some(version_num)))
            } else {
                Ok((None, None, None))
            }
        })
    });

    let metadata_tuples = runtime_handle.block_on(async move { join_all(handles_iter).await });
    let all_tuples = metadata_tuples
        .into_iter()
        .zip(values.iter())
        .map(|(t, u)| {
            t.with_context(|_| JoinSnafu {
                path: u.unwrap().to_string(),
            })?
        })
        .collect::<DaftResult<Vec<_>>>()?;
    assert_eq!(all_tuples.len(), uris.len());

    let row_count_series = UInt64Array::from((
        "row_count",
        Box::new(arrow2::array::UInt64Array::from_iter(
            all_tuples.iter().map(|v| v.0.map(|v| v as u64)),
        )),
    ));
    let row_group_series = UInt64Array::from((
        "row_group_count",
        Box::new(arrow2::array::UInt64Array::from_iter(
            all_tuples.iter().map(|v| v.1.map(|v| v as u64)),
        )),
    ));
    let version_series = Int32Array::from((
        "version",
        Box::new(arrow2::array::Int32Array::from_iter(
            all_tuples.iter().map(|v| v.2),
        )),
    ));

    Table::from_columns(vec![
        uris.clone(),
        row_count_series.into_series(),
        row_group_series.into_series(),
        version_series.into_series(),
    ])
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

        let table = read_parquet(file, None, None, None, io_client, &Default::default())?;
        assert_eq!(table.len(), 100);

        Ok(())
    }
}
