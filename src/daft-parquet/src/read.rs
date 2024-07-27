use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use arrow2::{bitmap::Bitmap, io::parquet::read::schema::infer_schema_with_options};
use common_error::DaftResult;

use daft_core::{
    datatypes::{BooleanArray, Field, Int32Array, TimeUnit, UInt64Array, Utf8Array},
    schema::Schema,
    DataType, IntoSeries, Series,
};
use daft_dsl::{optimization::get_required_columns, ExprRef};
use daft_io::{get_runtime, parse_url, IOClient, IOStatsRef, SourceType};
use daft_table::Table;
use futures::{
    future::{join_all, try_join_all},
    StreamExt, TryStreamExt,
};
use itertools::Itertools;
use parquet2::metadata::FileMetaData;
use snafu::ResultExt;
use tokio::runtime::Runtime;

use crate::{file::ParquetReaderBuilder, JoinSnafu};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Serialize, Deserialize)]
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

/// Returns the new number of rows to read after taking into account rows that need to be deleted after reading
fn limit_with_delete_rows(
    delete_rows: &[i64],
    start_offset: Option<usize>,
    num_rows_to_read: Option<usize>,
) -> Option<usize> {
    if let Some(mut n) = num_rows_to_read {
        let mut delete_rows_sorted = if let Some(start_offset) = start_offset {
            delete_rows
                .iter()
                .filter_map(|r| {
                    let shifted_row = *r - start_offset as i64;
                    if shifted_row >= 0 {
                        Some(shifted_row as usize)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        } else {
            delete_rows.iter().map(|r| *r as usize).collect::<Vec<_>>()
        };
        delete_rows_sorted.sort();
        delete_rows_sorted.dedup();

        for r in delete_rows_sorted {
            if r < n {
                n += 1;
            } else {
                break;
            }
        }

        Some(n)
    } else {
        num_rows_to_read
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_parquet_single(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    metadata: Option<Arc<FileMetaData>>,
    delete_rows: Option<Vec<i64>>,
) -> DaftResult<Table> {
    let field_id_mapping_provided = field_id_mapping.is_some();
    let columns_to_return = columns;
    let num_rows_to_return = num_rows;
    let mut num_rows_to_read = num_rows;
    let mut columns_to_read = columns.map(|s| s.iter().map(|s| s.to_string()).collect_vec());
    let requested_columns = columns_to_read.as_ref().map(|v| v.len());
    if let Some(ref pred) = predicate {
        num_rows_to_read = None;

        if let Some(req_columns) = columns_to_read.as_mut() {
            let needed_columns = get_required_columns(pred);
            for c in needed_columns {
                if !req_columns.contains(&c) {
                    req_columns.push(c);
                }
            }
        }
    }

    // Increase the number of rows_to_read to account for deleted rows
    // in order to have the correct number of rows in the end
    if let Some(delete_rows) = &delete_rows {
        num_rows_to_read = limit_with_delete_rows(delete_rows, start_offset, num_rows_to_read);
    }

    let (source_type, fixed_uri) = parse_url(uri)?;

    let (metadata, mut table) = if matches!(source_type, SourceType::File) {
        crate::stream_reader::local_parquet_read_async(
            fixed_uri.as_ref(),
            columns_to_read,
            start_offset,
            num_rows_to_read,
            row_groups.clone(),
            predicate.clone(),
            schema_infer_options,
            metadata,
        )
        .await
    } else {
        let builder = ParquetReaderBuilder::from_uri(
            uri,
            io_client.clone(),
            io_stats.clone(),
            field_id_mapping,
        )
        .await?;
        let builder = builder.set_infer_schema_options(schema_infer_options);

        let builder = if let Some(columns) = columns_to_read.as_ref() {
            builder.prune_columns(columns.as_slice())?
        } else {
            builder
        };

        if row_groups.is_some() && (num_rows_to_read.is_some() || start_offset.is_some()) {
            return Err(common_error::DaftError::ValueError("Both `row_groups` and `num_rows` or `start_offset` is set at the same time. We only support setting one set or the other.".to_string()));
        }
        let builder = builder.limit(start_offset, num_rows_to_read)?;
        let metadata = builder.metadata().clone();

        let builder = if let Some(ref row_groups) = row_groups {
            builder.set_row_groups(row_groups)?
        } else {
            builder
        };

        let builder = if let Some(ref predicate) = predicate {
            builder.set_filter(predicate.clone())
        } else {
            builder
        };

        let parquet_reader = builder.build()?;
        let ranges = parquet_reader.prebuffer_ranges(io_client, io_stats)?;
        Ok((
            Arc::new(metadata),
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

    let num_deleted_rows = if let Some(delete_rows) = delete_rows
        && !delete_rows.is_empty()
    {
        assert!(
            row_groups.is_none(),
            "Row group splitting is not supported with Iceberg deletion files."
        );

        let mut selection_mask = Bitmap::new_trued(table.len()).make_mut();

        let start_offset = start_offset.unwrap_or(0);

        for row in delete_rows.into_iter().map(|r| r as usize) {
            if row >= start_offset && num_rows_to_read.map_or(true, |n| row < start_offset + n) {
                let table_row = row - start_offset;

                if table_row < table.len() {
                    unsafe {
                        selection_mask.set_unchecked(table_row, false);
                    }
                } else {
                    return Err(super::Error::ParquetDeleteRowOutOfIndex {
                        path: uri.into(),
                        row: table_row,
                        read_rows: table.len(),
                    }
                    .into());
                }
            }
        }

        let num_deleted_rows = selection_mask.unset_bits();

        let selection_mask: BooleanArray = ("selection_mask", Bitmap::from(selection_mask)).into();

        table = table.mask_filter(&selection_mask.into_series())?;

        num_deleted_rows
    } else {
        0
    };

    if let Some(predicate) = predicate {
        // If a predicate exists, we need to apply it before a limit and also keep all of the columns that it needs until it is applied
        // TODO ideally pipeline this with IO and before concatenating, rather than after
        table = table.filter(&[predicate])?;
        if let Some(oc) = columns_to_return {
            table = table.get_columns(oc)?;
        }
        if let Some(nr) = num_rows_to_return {
            table = table.head(nr)?;
        }
    } else if let Some(row_groups) = row_groups {
        let expected_rows = row_groups
            .iter()
            .map(|i| rows_per_row_groups.get(*i as usize).unwrap())
            .sum::<usize>()
            - num_deleted_rows;
        if expected_rows != table.len() {
            return Err(super::Error::ParquetNumRowMismatch {
                path: uri.into(),
                expected_rows,
                read_rows: table.len(),
            }
            .into());
        }
    } else {
        let expected_rows = metadata_num_rows - num_deleted_rows;
        match (start_offset, num_rows_to_read) {
            (None, None) if expected_rows != table.len() => {
                Err(super::Error::ParquetNumRowMismatch {
                    path: uri.into(),
                    expected_rows,
                    read_rows: table.len(),
                })
            }
            (Some(s), None) if expected_rows.saturating_sub(s) != table.len() => {
                Err(super::Error::ParquetNumRowMismatch {
                    path: uri.into(),
                    expected_rows: expected_rows.saturating_sub(s),
                    read_rows: table.len(),
                })
            }
            (_, Some(n)) if n < table.len() => Err(super::Error::ParquetNumRowMismatch {
                path: uri.into(),
                expected_rows: n.min(expected_rows),
                read_rows: table.len(),
            }),
            _ => Ok(()),
        }?;
    }

    let expected_num_columns = if let Some(columns) = requested_columns {
        columns
    } else {
        metadata_num_columns
    };

    if (!field_id_mapping_provided
        && requested_columns.is_none()
        && table.num_columns() != expected_num_columns)
        || (field_id_mapping_provided && table.num_columns() > expected_num_columns)
        || (requested_columns.is_some() && table.num_columns() > expected_num_columns)
    {
        return Err(super::Error::ParquetNumColumnMismatch {
            path: uri.into(),
            metadata_num_columns: expected_num_columns,
            read_columns: table.num_columns(),
        }
        .into());
    }

    Ok(table)
}

#[allow(clippy::too_many_arguments)]
async fn read_parquet_single_into_arrow(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    metadata: Option<Arc<FileMetaData>>,
) -> DaftResult<(arrow2::datatypes::SchemaRef, Vec<ArrowChunk>, usize)> {
    let field_id_mapping_provided = field_id_mapping.is_some();
    let (source_type, fixed_uri) = parse_url(uri)?;
    let (metadata, schema, all_arrays, num_rows_read) = if matches!(source_type, SourceType::File) {
        let (metadata, schema, all_arrays, num_rows_read) =
            crate::stream_reader::local_parquet_read_into_arrow_async(
                fixed_uri.as_ref(),
                columns.map(|s| s.iter().map(|s| s.to_string()).collect_vec()),
                start_offset,
                num_rows,
                row_groups.clone(),
                None,
                schema_infer_options,
                metadata,
            )
            .await?;
        (metadata, Arc::new(schema), all_arrays, num_rows_read)
    } else {
        let builder = ParquetReaderBuilder::from_uri(
            uri,
            io_client.clone(),
            io_stats.clone(),
            field_id_mapping,
        )
        .await?;
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
        let ranges = parquet_reader.prebuffer_ranges(io_client, io_stats)?;
        let (all_arrays, num_rows_read) = parquet_reader
            .read_from_ranges_into_arrow_arrays(ranges)
            .await?;
        (Arc::new(metadata), schema, all_arrays, num_rows_read)
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
        .map(|v| v.iter().map(|a| a.len()).sum::<usize>())
        .collect::<Vec<_>>();
    let all_same_size = len_per_col.windows(2).all(|w| w[0] == w[1]);
    if !all_same_size {
        return Err(super::Error::ParquetColumnsDontHaveEqualRows { path: uri.into() }.into());
    }

    let table_len = num_rows_read;
    let table_ncol = all_arrays.len();

    if let Some(row_groups) = &row_groups {
        let expected_rows: usize = row_groups
            .iter()
            .map(|i| rows_per_row_groups.get(*i as usize).unwrap())
            .sum();
        if expected_rows != table_len {
            return Err(super::Error::ParquetNumRowMismatch {
                path: uri.into(),
                expected_rows,
                read_rows: table_len,
            }
            .into());
        }
    } else {
        match (start_offset, num_rows) {
            (None, None) if metadata_num_rows != table_len => {
                Err(super::Error::ParquetNumRowMismatch {
                    path: uri.into(),
                    expected_rows: metadata_num_rows,
                    read_rows: table_len,
                })
            }
            (Some(s), None) if metadata_num_rows.saturating_sub(s) != table_len => {
                Err(super::Error::ParquetNumRowMismatch {
                    path: uri.into(),
                    expected_rows: metadata_num_rows.saturating_sub(s),
                    read_rows: table_len,
                })
            }
            (_, Some(n)) if n < table_len => Err(super::Error::ParquetNumRowMismatch {
                path: uri.into(),
                expected_rows: n.min(metadata_num_rows),
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

    if (!field_id_mapping_provided && columns.is_none() && table_ncol != expected_num_columns)
        || (field_id_mapping_provided && table_ncol > expected_num_columns)
        || (columns.is_some() && table_ncol > expected_num_columns)
    {
        return Err(super::Error::ParquetNumColumnMismatch {
            path: uri.into(),
            metadata_num_columns: expected_num_columns,
            read_columns: table_ncol,
        }
        .into());
    }

    Ok((schema, all_arrays, table_len))
}

#[allow(clippy::too_many_arguments)]
pub fn read_parquet(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    runtime_handle: Arc<Runtime>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<FileMetaData>>,
) -> DaftResult<Table> {
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        read_parquet_single(
            uri,
            columns,
            start_offset,
            num_rows,
            row_groups,
            predicate,
            io_client,
            io_stats,
            schema_infer_options,
            None,
            metadata,
            None,
        )
        .await
    })
}
pub type ArrowChunk = Vec<Box<dyn arrow2::array::Array>>;
pub type ParquetPyarrowChunk = (arrow2::datatypes::SchemaRef, Vec<ArrowChunk>, usize);
#[allow(clippy::too_many_arguments)]
pub fn read_parquet_into_pyarrow(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    runtime_handle: Arc<Runtime>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    file_timeout_ms: Option<i64>,
) -> DaftResult<ParquetPyarrowChunk> {
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        let fut = read_parquet_single_into_arrow(
            uri,
            columns,
            start_offset,
            num_rows,
            row_groups,
            io_client,
            io_stats,
            schema_infer_options,
            None,
            None,
        );
        if let Some(timeout) = file_timeout_ms {
            match tokio::time::timeout(Duration::from_millis(timeout as u64), fut).await {
                Ok(result) => result,
                Err(_) => Err(crate::Error::FileReadTimeout {
                    path: uri.to_string(),
                    duration_ms: timeout,
                }
                .into()),
            }
        } else {
            fut.await
        }
    })
}

#[allow(clippy::too_many_arguments)]
pub fn read_parquet_bulk(
    uris: &[&str],
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<Option<Vec<i64>>>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    runtime_handle: Arc<Runtime>,
    schema_infer_options: &ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    metadata: Option<Vec<Arc<FileMetaData>>>,
    delete_map: Option<HashMap<String, Vec<i64>>>,
) -> DaftResult<Vec<Table>> {
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
                let owned_row_group = row_groups.as_ref().and_then(|rgs| rgs[i].clone());
                let owned_predicate = predicate.clone();
                let metadata = metadata.as_ref().map(|mds| mds[i].clone());

                let io_client = io_client.clone();
                let io_stats = io_stats.clone();
                let schema_infer_options = *schema_infer_options;
                let owned_field_id_mapping = field_id_mapping.clone();
                let delete_rows = delete_map.as_ref().and_then(|m| m.get(&uri).cloned());
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
                            owned_predicate,
                            io_client,
                            io_stats,
                            schema_infer_options,
                            owned_field_id_mapping,
                            metadata,
                            delete_rows,
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
    row_groups: Option<Vec<Option<Vec<i64>>>>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
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
                let owned_row_group = row_groups.as_ref().and_then(|rgs| rgs[i].clone());

                let io_client = io_client.clone();
                let io_stats = io_stats.clone();

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
                            io_stats,
                            schema_infer_options,
                            None,
                            None,
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
    io_stats: Option<IOStatsRef>,
    schema_inference_options: ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<(Schema, FileMetaData)> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    let builder = runtime_handle.block_on(async {
        ParquetReaderBuilder::from_uri(uri, io_client.clone(), io_stats, field_id_mapping).await
    })?;
    let builder = builder.set_infer_schema_options(schema_inference_options);

    let metadata = builder.metadata;
    let arrow_schema =
        infer_schema_with_options(&metadata, &Some(schema_inference_options.into()))?;
    let schema = Schema::try_from(&arrow_schema)?;
    Ok((schema, metadata))
}

pub async fn read_parquet_metadata(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<parquet2::metadata::FileMetaData> {
    let builder =
        ParquetReaderBuilder::from_uri(uri, io_client, io_stats, field_id_mapping).await?;
    Ok(builder.metadata)
}
pub async fn read_parquet_metadata_bulk(
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<Vec<parquet2::metadata::FileMetaData>> {
    let handles_iter = uris.iter().map(|uri| {
        let owned_string = uri.to_string();
        let owned_client = io_client.clone();
        let owned_io_stats = io_stats.clone();
        let owned_field_id_mapping = field_id_mapping.clone();
        tokio::spawn(async move {
            read_parquet_metadata(
                &owned_string,
                owned_client,
                owned_io_stats,
                owned_field_id_mapping,
            )
            .await
        })
    });
    let all_metadatas = try_join_all(handles_iter)
        .await
        .context(JoinSnafu { path: "BULK READ" })?;
    all_metadatas.into_iter().collect::<DaftResult<Vec<_>>>()
}

pub fn read_parquet_statistics(
    uris: &Series,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<Table> {
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
        let io_stats = io_stats.clone();
        let owned_field_id_mapping = field_id_mapping.clone();

        tokio::spawn(async move {
            if let Some(owned_string) = owned_string {
                let metadata = read_parquet_metadata(
                    &owned_string,
                    owned_client,
                    io_stats,
                    owned_field_id_mapping,
                )
                .await?;
                let num_rows = metadata.num_rows;
                let num_row_groups = metadata.row_groups.len();
                let version_num = metadata.version;

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

    Table::from_nonempty_columns(vec![
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
        let runtime_handle = daft_io::get_runtime(true)?;

        let table = read_parquet(
            file,
            None,
            None,
            None,
            None,
            None,
            io_client,
            None,
            runtime_handle,
            Default::default(),
            None,
        )?;
        assert_eq!(table.len(), 100);

        Ok(())
    }
}
