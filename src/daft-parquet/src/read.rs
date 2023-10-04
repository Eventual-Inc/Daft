use std::sync::Arc;

use common_error::DaftResult;

use daft_core::{
    datatypes::{Int32Array, TimeUnit, UInt64Array, Utf8Array},
    schema::Schema,
    DataType, IntoSeries, Series,
};
use daft_io::{get_runtime, parse_url, IOClient, SourceType};
use daft_table::Table;
use futures::{future::join_all, StreamExt, TryStreamExt};
use itertools::Itertools;
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

impl From<ParquetSchemaInferenceOptions>
    for arrow2::io::parquet::read::schema::SchemaInferenceOptions
{
    fn from(value: ParquetSchemaInferenceOptions) -> Self {
        arrow2::io::parquet::read::schema::SchemaInferenceOptions {
            int96_coerce_to_timeunit: value.coerce_int96_timestamp_unit.to_arrow(),
        }
    }
}

async fn read_parquet_single(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    io_client: Arc<IOClient>,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> DaftResult<Table> {
    let (source_type, fixed_uri) = parse_url(uri)?;
    let (metadata, table) = if matches!(source_type, SourceType::File) {
        crate::stream_reader::local_parquet_read_async(
            fixed_uri.as_ref(),
            columns.map(|s| s.iter().map(|s| s.to_string()).collect_vec()),
            start_offset,
            num_rows,
            row_groups.clone(),
            schema_infer_options,
        )
        .await
    } else {
        let builder = ParquetReaderBuilder::from_uri(uri, io_client.clone()).await?;
        let builder = builder.set_infer_schema_options(schema_infer_options);

        let builder = if let Some(columns) = columns {
            builder.prune_columns(columns)?
        } else {
            builder
        };

        if row_groups.is_some() && (num_rows.is_some() || start_offset.is_some()) {
            return Err(common_error::DaftError::ValueError("Both `row_groups` and `num_rows` or `start_offset` is set at the same time. We only support setting one set or the other.".to_string()));
        }
        let builder = builder.limit(start_offset, num_rows)?;
        let metadata = builder.metadata().clone();

        let builder = if let Some(ref row_groups) = row_groups {
            builder.set_row_groups(row_groups)?
        } else {
            builder
        };

        let parquet_reader = builder.build()?;
        let ranges = parquet_reader.prebuffer_ranges(io_client)?;
        Ok((
            metadata,
            parquet_reader.read_from_ranges_into_table(ranges).await?,
        ))
    }?;

    let rows_per_row_groups = metadata
        .row_groups
        .iter()
        .map(|m| m.num_rows())
        .collect::<Vec<_>>();

    let metadata_num_rows = metadata.num_rows;

    let metadata_num_columns = metadata.schema().fields().len();

    if let Some(row_groups) = row_groups {
        let expected_rows: usize = row_groups
            .iter()
            .map(|i| rows_per_row_groups.get(*i as usize).unwrap())
            .sum();
        if expected_rows != table.len() {
            return Err(super::Error::ParquetNumRowMismatch {
                path: uri.into(),
                metadata_num_rows: expected_rows,
                read_rows: table.len(),
            }
            .into());
        }
    } else {
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
    };

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

async fn read_parquet_single_into_arrow(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    io_client: Arc<IOClient>,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> DaftResult<(arrow2::datatypes::SchemaRef, Vec<ArrowChunk>)> {
    let (source_type, fixed_uri) = parse_url(uri)?;
    let (metadata, schema, all_arrays) = if matches!(source_type, SourceType::File) {
        let (metadata, schema, all_arrays) =
            crate::stream_reader::local_parquet_read_into_arrow_async(
                fixed_uri.as_ref(),
                columns.map(|s| s.iter().map(|s| s.to_string()).collect_vec()),
                start_offset,
                num_rows,
                row_groups.clone(),
                schema_infer_options,
            )
            .await?;
        (metadata, Arc::new(schema), all_arrays)
    } else {
        let builder = ParquetReaderBuilder::from_uri(uri, io_client.clone()).await?;
        let builder = builder.set_infer_schema_options(schema_infer_options);

        let builder = if let Some(columns) = columns {
            builder.prune_columns(columns)?
        } else {
            builder
        };

        if row_groups.is_some() && (num_rows.is_some() || start_offset.is_some()) {
            return Err(common_error::DaftError::ValueError("Both `row_groups` and `num_rows` or `start_offset` is set at the same time. We only support setting one set or the other.".to_string()));
        }
        let builder = builder.limit(start_offset, num_rows)?;
        let metadata = builder.metadata().clone();

        let builder = if let Some(ref row_groups) = row_groups {
            builder.set_row_groups(row_groups)?
        } else {
            builder
        };

        let parquet_reader = builder.build()?;

        let schema = parquet_reader.arrow_schema().clone();
        let ranges = parquet_reader.prebuffer_ranges(io_client)?;
        let all_arrays = parquet_reader
            .read_from_ranges_into_arrow_arrays(ranges)
            .await?;
        (metadata, schema, all_arrays)
    };

    let rows_per_row_groups = metadata
        .row_groups
        .iter()
        .map(|m| m.num_rows())
        .collect::<Vec<_>>();

    let metadata_num_rows = metadata.num_rows;

    let metadata_num_columns = metadata.schema().fields().len();

    let len_per_col = all_arrays
        .iter()
        .map(|v| v.iter().map(|a| a.len()).sum())
        .collect::<Vec<_>>();
    let all_same_size = len_per_col.windows(2).all(|w| w[0] == w[1]);
    if !all_same_size {
        return Err(super::Error::ParquetColumnsDontHaveEqualRows { path: uri.into() }.into());
    }

    let table_len = *len_per_col.first().unwrap_or(&0);

    let table_ncol = all_arrays.len();

    if let Some(row_groups) = &row_groups {
        let expected_rows: usize = row_groups
            .iter()
            .map(|i| rows_per_row_groups.get(*i as usize).unwrap())
            .sum();
        if expected_rows != table_len {
            return Err(super::Error::ParquetNumRowMismatch {
                path: uri.into(),
                metadata_num_rows: expected_rows,
                read_rows: table_len,
            }
            .into());
        }
    } else {
        match (start_offset, num_rows) {
            (None, None) if metadata_num_rows != table_len => {
                Err(super::Error::ParquetNumRowMismatch {
                    path: uri.into(),
                    metadata_num_rows,
                    read_rows: table_len,
                })
            }
            (Some(s), None) if metadata_num_rows.saturating_sub(s) != table_len => {
                Err(super::Error::ParquetNumRowMismatch {
                    path: uri.into(),
                    metadata_num_rows: metadata_num_rows.saturating_sub(s),
                    read_rows: table_len,
                })
            }
            (_, Some(n)) if n < table_len => Err(super::Error::ParquetNumRowMismatch {
                path: uri.into(),
                metadata_num_rows: n.min(metadata_num_rows),
                read_rows: table_len,
            }),
            _ => Ok(()),
        }?;
    };

    let expected_num_columns = if let Some(columns) = columns {
        columns.len()
    } else {
        metadata_num_columns
    };

    if table_ncol != expected_num_columns {
        return Err(super::Error::ParquetNumColumnMismatch {
            path: uri.into(),
            metadata_num_columns: expected_num_columns,
            read_columns: table_ncol,
        }
        .into());
    }

    Ok((schema, all_arrays))
}

#[allow(clippy::too_many_arguments)]
pub fn read_parquet(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    io_client: Arc<IOClient>,
    multithreaded_io: bool,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(multithreaded_io)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        read_parquet_single(
            uri,
            columns,
            start_offset,
            num_rows,
            row_groups,
            io_client,
            schema_infer_options,
        )
        .await
    })
}
pub type ArrowChunk = Vec<Box<dyn arrow2::array::Array>>;
pub type ParquetPyarrowChunk = (arrow2::datatypes::SchemaRef, Vec<ArrowChunk>);
#[allow(clippy::too_many_arguments)]
pub fn read_parquet_into_pyarrow(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    io_client: Arc<IOClient>,
    multithreaded_io: bool,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> DaftResult<ParquetPyarrowChunk> {
    let runtime_handle = get_runtime(multithreaded_io)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        read_parquet_single_into_arrow(
            uri,
            columns,
            start_offset,
            num_rows,
            row_groups,
            io_client,
            schema_infer_options,
        )
        .await
    })
}

#[allow(clippy::too_many_arguments)]
pub fn read_parquet_bulk(
    uris: &[&str],
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<Vec<i64>>>,
    io_client: Arc<IOClient>,
    num_parallel_tasks: usize,
    multithreaded_io: bool,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<Vec<Table>> {
    let runtime_handle = get_runtime(multithreaded_io)?;
    let _rt_guard = runtime_handle.enter();
    let owned_columns = columns.map(|s| s.iter().map(|v| String::from(*v)).collect::<Vec<_>>());
    if let Some(ref row_groups) = row_groups {
        if row_groups.len() != uris.len() {
            return Err(common_error::DaftError::ValueError(format!(
                "Mismatch of length of `uris` and `row_groups`. {} vs {}",
                uris.len(),
                row_groups.len()
            )));
        }
    }
    let tables = runtime_handle
        .block_on(async move {
            let task_stream = futures::stream::iter(uris.iter().enumerate().map(|(i, uri)| {
                let uri = uri.to_string();
                let owned_columns = owned_columns.clone();
                let owned_row_group = match &row_groups {
                    None => None,
                    Some(v) => v.get(i).cloned(),
                };

                let io_client = io_client.clone();
                let schema_infer_options = schema_infer_options.clone();
                tokio::task::spawn(async move {
                    let columns = owned_columns
                        .as_ref()
                        .map(|s| s.iter().map(AsRef::as_ref).collect::<Vec<_>>());
                    Ok((
                        i,
                        read_parquet_single(
                            &uri,
                            columns.as_deref(),
                            start_offset,
                            num_rows,
                            owned_row_group,
                            io_client,
                            schema_infer_options,
                        )
                        .await?,
                    ))
                })
            }));
            task_stream
                .buffer_unordered(num_parallel_tasks)
                .try_collect::<Vec<_>>()
                .await
        })
        .context(JoinSnafu { path: "UNKNOWN" })?;

    let mut collected = tables.into_iter().collect::<DaftResult<Vec<_>>>()?;
    collected.sort_by_key(|(idx, _)| *idx);
    Ok(collected.into_iter().map(|(_, v)| v).collect())
}

#[allow(clippy::too_many_arguments)]
pub fn read_parquet_into_pyarrow_bulk(
    uris: &[&str],
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<Vec<i64>>>,
    io_client: Arc<IOClient>,
    num_parallel_tasks: usize,
    multithreaded_io: bool,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> DaftResult<Vec<ParquetPyarrowChunk>> {
    let runtime_handle = get_runtime(multithreaded_io)?;
    let _rt_guard = runtime_handle.enter();
    let owned_columns = columns.map(|s| s.iter().map(|v| String::from(*v)).collect::<Vec<_>>());
    if let Some(ref row_groups) = row_groups {
        if row_groups.len() != uris.len() {
            return Err(common_error::DaftError::ValueError(format!(
                "Mismatch of length of `uris` and `row_groups`. {} vs {}",
                uris.len(),
                row_groups.len()
            )));
        }
    }
    let tables = runtime_handle
        .block_on(async move {
            futures::stream::iter(uris.iter().enumerate().map(|(i, uri)| {
                let uri = uri.to_string();
                let owned_columns = owned_columns.clone();
                let schema_infer_options = schema_infer_options.clone();
                let owned_row_group = match &row_groups {
                    None => None,
                    Some(v) => v.get(i).cloned(),
                };

                let io_client = io_client.clone();
                let schema_infer_options = schema_infer_options.clone();
                tokio::task::spawn(async move {
                    let columns = owned_columns
                        .as_ref()
                        .map(|s| s.iter().map(AsRef::as_ref).collect::<Vec<_>>());
                    Ok((
                        i,
                        read_parquet_single_into_arrow(
                            &uri,
                            columns.as_deref(),
                            start_offset,
                            num_rows,
                            owned_row_group,
                            io_client,
                            schema_infer_options,
                        )
                        .await?,
                    ))
                })
            }))
            .buffer_unordered(num_parallel_tasks)
            .try_collect::<Vec<_>>()
            .await
        })
        .context(JoinSnafu { path: "UNKNOWN" })?;
    let mut collected = tables.into_iter().collect::<DaftResult<Vec<_>>>()?;
    collected.sort_by_key(|(idx, _)| *idx);
    Ok(collected.into_iter().map(|(_, v)| v).collect())
}

pub fn read_parquet_schema(
    uri: &str,
    io_client: Arc<IOClient>,
    schema_inference_options: ParquetSchemaInferenceOptions,
) -> DaftResult<Schema> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    let builder = runtime_handle
        .block_on(async { ParquetReaderBuilder::from_uri(uri, io_client.clone()).await })?;
    let builder = builder.set_infer_schema_options(schema_inference_options);

    Schema::try_from(builder.build()?.arrow_schema().as_ref())
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

    use daft_io::{IOClient, IOConfig};

    use super::read_parquet;
    #[test]
    fn test_parquet_read_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_parquet(
            file,
            None,
            None,
            None,
            None,
            io_client,
            true,
            Default::default(),
        )?;
        assert_eq!(table.len(), 100);

        Ok(())
    }
}
