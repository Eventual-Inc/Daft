use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use arrow2::{
    bitmap::Bitmap,
    io::parquet::read::schema::{
        infer_schema_with_options, SchemaInferenceOptions, StringEncoding,
    },
};
use common_error::DaftResult;
use common_runtime::get_io_runtime;
use daft_core::prelude::*;
#[cfg(feature = "python")]
use daft_core::python::PyTimeUnit;
use daft_dsl::{optimization::get_required_columns, ExprRef};
use daft_io::{parse_url, IOClient, IOStatsRef, SourceType};
use daft_table::Table;
use futures::{
    future::{join_all, try_join_all},
    stream::BoxStream,
    Stream, StreamExt, TryStreamExt,
};
use itertools::Itertools;
use parquet2::metadata::FileMetaData;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{file::ParquetReaderBuilder, JoinSnafu};

#[cfg(feature = "python")]
#[derive(Clone)]
pub struct ParquetSchemaInferenceOptionsBuilder {
    pub coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    pub string_encoding: String,
}

#[cfg(feature = "python")]
impl ParquetSchemaInferenceOptionsBuilder {
    pub fn build(self) -> crate::Result<ParquetSchemaInferenceOptions> {
        self.try_into()
    }
}

#[cfg(feature = "python")]
impl TryFrom<ParquetSchemaInferenceOptionsBuilder> for ParquetSchemaInferenceOptions {
    type Error = crate::Error;

    fn try_from(value: ParquetSchemaInferenceOptionsBuilder) -> crate::Result<Self> {
        Ok(Self {
            coerce_int96_timestamp_unit: value
                .coerce_int96_timestamp_unit
                .map_or(TimeUnit::Nanoseconds, From::from),
            string_encoding: value.string_encoding.parse().context(crate::Arrow2Snafu)?,
        })
    }
}

#[cfg(feature = "python")]
impl Default for ParquetSchemaInferenceOptionsBuilder {
    fn default() -> Self {
        Self {
            coerce_int96_timestamp_unit: Some(PyTimeUnit::nanoseconds().unwrap()),
            string_encoding: "utf-8".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ParquetSchemaInferenceOptions {
    pub coerce_int96_timestamp_unit: TimeUnit,
    pub string_encoding: StringEncoding,
}

impl ParquetSchemaInferenceOptions {
    #[must_use]
    pub fn new(coerce_int96_timestamp_unit: Option<TimeUnit>) -> Self {
        let coerce_int96_timestamp_unit =
            coerce_int96_timestamp_unit.unwrap_or(TimeUnit::Nanoseconds);
        Self {
            coerce_int96_timestamp_unit,
            ..Default::default()
        }
    }
}

impl Default for ParquetSchemaInferenceOptions {
    fn default() -> Self {
        Self {
            coerce_int96_timestamp_unit: TimeUnit::Nanoseconds,
            string_encoding: StringEncoding::Utf8,
        }
    }
}

impl From<ParquetSchemaInferenceOptions> for SchemaInferenceOptions {
    fn from(value: ParquetSchemaInferenceOptions) -> Self {
        Self {
            int96_coerce_to_timeunit: value.coerce_int96_timestamp_unit.to_arrow(),
            string_encoding: value.string_encoding,
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
        delete_rows_sorted.sort_unstable();
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
    columns: Option<Vec<String>>,
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
    chunk_size: Option<usize>,
) -> DaftResult<Table> {
    let field_id_mapping_provided = field_id_mapping.is_some();
    let mut columns_to_read = columns.clone();
    let columns_to_return = columns;
    let num_rows_to_return = num_rows;
    let mut num_rows_to_read = num_rows;
    let requested_columns = columns_to_read.as_ref().map(std::vec::Vec::len);
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
            chunk_size,
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

        let builder = if let Some(columns) = &columns_to_read {
            builder.prune_columns(columns)?
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

        let builder = builder.set_chunk_size(chunk_size);

        let parquet_reader = builder.build()?;
        let ranges = parquet_reader.prebuffer_ranges(io_client, io_stats)?;
        Ok((
            Arc::new(metadata),
            parquet_reader.read_from_ranges_into_table(ranges).await?,
        ))
    }?;

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
            table = table.get_columns(&oc)?;
        }
        if let Some(nr) = num_rows_to_return {
            table = table.head(nr)?;
        }
    } else if let Some(row_groups) = row_groups {
        let expected_rows = row_groups
            .iter()
            .map(|i| metadata.row_groups.get(&(*i as usize)).unwrap().num_rows())
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
async fn stream_parquet_single(
    uri: String,
    columns: Option<&[&str]>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    metadata: Option<Arc<FileMetaData>>,
    delete_rows: Option<Vec<i64>>,
    maintain_order: bool,
) -> DaftResult<impl Stream<Item = DaftResult<Table>> + Send> {
    let field_id_mapping_provided = field_id_mapping.is_some();
    let columns_to_return = columns.map(|s| s.iter().map(|s| (*s).to_string()).collect_vec());
    let num_rows_to_return = num_rows;
    let mut num_rows_to_read = num_rows;
    let mut columns_to_read = columns.map(|s| s.iter().map(|s| (*s).to_string()).collect_vec());
    let requested_columns = columns_to_read.as_ref().map(std::vec::Vec::len);
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
        num_rows_to_read = limit_with_delete_rows(delete_rows, None, num_rows_to_read);
    }

    let (source_type, fixed_uri) = parse_url(uri.as_str())?;

    let (metadata, table_stream) = if matches!(source_type, SourceType::File) {
        crate::stream_reader::local_parquet_stream(
            fixed_uri.as_ref(),
            columns_to_return,
            columns_to_read,
            num_rows_to_return,
            num_rows_to_read,
            delete_rows,
            row_groups.clone(),
            predicate.clone(),
            schema_infer_options,
            metadata,
            maintain_order,
            io_stats,
        )
    } else {
        let builder = ParquetReaderBuilder::from_uri(
            uri.as_str(),
            io_client.clone(),
            io_stats.clone(),
            field_id_mapping,
        )
        .await?;
        let builder = builder.set_infer_schema_options(schema_infer_options);

        let builder = if let Some(columns) = &columns_to_read {
            builder.prune_columns(columns)?
        } else {
            builder
        };

        if row_groups.is_some() && num_rows_to_read.is_some() {
            return Err(common_error::DaftError::ValueError("Both `row_groups` and `num_rows` is set at the same time. We only support setting one set or the other.".to_string()));
        }
        let builder = builder.limit(None, num_rows_to_read)?;
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
            parquet_reader
                .read_from_ranges_into_table_stream(
                    ranges,
                    maintain_order,
                    predicate.clone(),
                    columns_to_return,
                    num_rows_to_return,
                    delete_rows,
                )
                .await?,
        ))
    }?;

    let metadata_num_columns = metadata.schema().fields().len();
    let mut remaining_rows = num_rows_to_return.map(|limit| limit as i64);
    let finalized_table_stream = table_stream
        .map(move |table| {
            let table = table?;

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
                    path: uri.to_string(),
                    metadata_num_columns: expected_num_columns,
                    read_columns: table.num_columns(),
                }
                .into());
            }
            DaftResult::Ok(table)
        })
        .try_take_while(move |table| {
            match remaining_rows {
                // Limit has been met, early-terminate.
                Some(rows_left) if rows_left <= 0 => futures::future::ready(Ok(false)),
                // Limit has not yet been met, update remaining limit slack and continue.
                Some(rows_left) => {
                    remaining_rows = Some(rows_left - table.len() as i64);
                    futures::future::ready(Ok(true))
                }
                // No limit, never early-terminate.
                None => futures::future::ready(Ok(true)),
            }
        });

    Ok(finalized_table_stream)
}

#[allow(clippy::too_many_arguments)]
async fn read_parquet_single_into_arrow(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    metadata: Option<Arc<FileMetaData>>,
) -> DaftResult<ParquetPyarrowChunk> {
    let field_id_mapping_provided = field_id_mapping.is_some();
    let (source_type, fixed_uri) = parse_url(uri)?;
    let (metadata, schema, all_arrays, num_rows_read) = if matches!(source_type, SourceType::File) {
        let (metadata, schema, all_arrays, num_rows_read) =
            crate::stream_reader::local_parquet_read_into_arrow_async(
                fixed_uri.as_ref(),
                columns.clone(),
                start_offset,
                num_rows,
                row_groups.clone(),
                None,
                schema_infer_options,
                metadata,
                None,
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

        let builder = if let Some(columns) = &columns {
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
        .values()
        .map(parquet2::metadata::RowGroupMetaData::num_rows)
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

    let expected_num_columns = if let Some(columns) = &columns {
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
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<FileMetaData>>,
) -> DaftResult<Table> {
    let runtime_handle = get_io_runtime(multithreaded_io);

    runtime_handle.block_on_current_thread(async {
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
            None,
        )
        .await
    })
}
pub type ArrowChunk = Vec<Box<dyn arrow2::array::Array>>;
pub type ArrowChunkIters = Vec<
    Box<dyn Iterator<Item = arrow2::error::Result<Box<dyn arrow2::array::Array>>> + Send + Sync>,
>;
pub type ParquetPyarrowChunk = (arrow2::datatypes::SchemaRef, Vec<ArrowChunk>, usize);
#[allow(clippy::too_many_arguments)]
pub fn read_parquet_into_pyarrow(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    schema_infer_options: ParquetSchemaInferenceOptions,
    file_timeout_ms: Option<i64>,
) -> DaftResult<ParquetPyarrowChunk> {
    let runtime_handle = get_io_runtime(multithreaded_io);
    runtime_handle.block_on_current_thread(async {
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
pub fn read_parquet_bulk<T: AsRef<str>>(
    uris: &[&str],
    columns: Option<&[T]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<Option<Vec<i64>>>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    multithreaded_io: bool,
    schema_infer_options: &ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    metadata: Option<Vec<Arc<FileMetaData>>>,
    delete_map: Option<HashMap<String, Vec<i64>>>,
    chunk_size: Option<usize>,
) -> DaftResult<Vec<Table>> {
    let runtime_handle = get_io_runtime(multithreaded_io);

    let columns = columns.map(|s| s.iter().map(|v| v.as_ref().to_string()).collect::<Vec<_>>());
    if let Some(ref row_groups) = row_groups {
        if row_groups.len() != uris.len() {
            return Err(common_error::DaftError::ValueError(format!(
                "Mismatch of length of `uris` and `row_groups`. {} vs {}",
                uris.len(),
                row_groups.len()
            )));
        }
    }

    let tables = runtime_handle.block_on_current_thread(read_parquet_bulk_async(
        uris.iter().map(|s| (*s).to_string()).collect(),
        columns,
        start_offset,
        num_rows,
        row_groups,
        predicate,
        io_client,
        io_stats,
        num_parallel_tasks,
        *schema_infer_options,
        field_id_mapping,
        metadata,
        delete_map,
        chunk_size,
    ))?;
    tables.into_iter().collect::<DaftResult<Vec<_>>>()
}

#[allow(clippy::too_many_arguments)]
pub async fn read_parquet_bulk_async(
    uris: Vec<String>,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<Option<Vec<i64>>>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    schema_infer_options: ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    metadata: Option<Vec<Arc<FileMetaData>>>,
    delete_map: Option<HashMap<String, Vec<i64>>>,
    chunk_size: Option<usize>,
) -> DaftResult<Vec<DaftResult<Table>>> {
    let task_stream = futures::stream::iter(uris.into_iter().enumerate().map(|(i, uri)| {
        let owned_columns = columns.clone();
        let owned_row_group = row_groups.as_ref().and_then(|rgs| rgs[i].clone());
        let owned_predicate = predicate.clone();
        let metadata = metadata.as_ref().map(|mds| mds[i].clone());

        let io_client = io_client.clone();
        let io_stats = io_stats.clone();
        let owned_field_id_mapping = field_id_mapping.clone();
        let delete_rows = delete_map.as_ref().and_then(|m| m.get(&uri).cloned());

        tokio::task::spawn(async move {
            read_parquet_single(
                &uri,
                owned_columns,
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
                chunk_size,
            )
            .await
        })
    }));

    let mut remaining_rows = num_rows.map(|x| x as i64);
    let tables = task_stream
        .buffered(num_parallel_tasks)
        .try_take_while(|result| match (result, remaining_rows) {
            (_, Some(rows_left)) if rows_left <= 0 => futures::future::ready(Ok(false)),
            (Ok(table), Some(rows_left)) => {
                remaining_rows = Some(rows_left - table.len() as i64);
                futures::future::ready(Ok(true))
            }
            (_, None) | (Err(_), _) => futures::future::ready(Ok(true)),
        })
        .try_collect::<Vec<_>>()
        .await
        .context(JoinSnafu { path: "UNKNOWN" })?;
    Ok(tables)
}

#[allow(clippy::too_many_arguments)]
pub async fn stream_parquet(
    uri: &str,
    columns: Option<&[&str]>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: &ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    metadata: Option<Arc<FileMetaData>>,
    maintain_order: bool,
    delete_rows: Option<Vec<i64>>,
) -> DaftResult<BoxStream<'static, DaftResult<Table>>> {
    let stream = stream_parquet_single(
        uri.to_string(),
        columns,
        num_rows,
        row_groups,
        predicate,
        io_client,
        io_stats,
        *schema_infer_options,
        field_id_mapping,
        metadata,
        delete_rows,
        maintain_order,
    )
    .await?;
    Ok(Box::pin(stream))
}

#[allow(clippy::too_many_arguments)]
pub fn read_parquet_into_pyarrow_bulk<T: AsRef<str>>(
    uris: &[&str],
    columns: Option<&[T]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<Option<Vec<i64>>>>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    multithreaded_io: bool,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> DaftResult<Vec<ParquetPyarrowChunk>> {
    let runtime_handle = get_io_runtime(multithreaded_io);
    let columns = columns.map(|s| s.iter().map(|v| v.as_ref().to_string()).collect::<Vec<_>>());
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
        .block_on_current_thread(async move {
            futures::stream::iter(uris.iter().enumerate().map(|(i, uri)| {
                let uri = (*uri).to_string();
                let owned_columns = columns.clone();
                let owned_row_group = row_groups.as_ref().and_then(|rgs| rgs[i].clone());

                let io_client = io_client.clone();
                let io_stats = io_stats.clone();

                tokio::task::spawn(async move {
                    Ok((
                        i,
                        read_parquet_single_into_arrow(
                            &uri,
                            owned_columns,
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
    let runtime_handle = get_io_runtime(true);
    let builder = runtime_handle.block_on_current_thread(async {
        ParquetReaderBuilder::from_uri(uri, io_client.clone(), io_stats, field_id_mapping).await
    })?;
    let builder = builder.set_infer_schema_options(schema_inference_options);

    let metadata = builder.metadata;
    let arrow_schema = infer_schema_with_options(&metadata, Some(schema_inference_options.into()))?;
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
        let owned_string = (*uri).to_string();
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
    let runtime_handle = get_io_runtime(true);

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
        let owned_string = uri.map(std::string::ToString::to_string);
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

    let metadata_tuples =
        runtime_handle.block_on_current_thread(async move { join_all(handles_iter).await });
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
    use std::{path::PathBuf, sync::Arc};

    use arrow2::{datatypes::DataType, io::parquet::read::schema::StringEncoding};
    use common_error::DaftResult;
    use daft_io::{IOClient, IOConfig};
    use futures::StreamExt;
    use parquet2::{
        metadata::FileMetaData,
        schema::types::{ParquetType, PrimitiveConvertedType, PrimitiveLogicalType},
    };

    use super::*;

    const PARQUET_FILE: &str = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
    const PARQUET_FILE_LOCAL: &str = "tests/assets/parquet-data/mvp.parquet";

    fn get_local_parquet_path() -> String {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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
        let runtime_handle = get_io_runtime(true);
        runtime_handle.block_on_current_thread(async move {
            let tables = stream_parquet(
                file,
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
                None,
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
        let runtime_handle = get_io_runtime(true);

        runtime_handle.block_on(async move {
            let metadata = read_parquet_metadata(&file, io_client, None, None).await?;
            let serialized = bincode::serialize(&metadata).unwrap();
            let deserialized = bincode::deserialize::<FileMetaData>(&serialized).unwrap();
            assert_eq!(metadata, deserialized);
            Ok(())
        })?
    }

    #[test]
    fn test_invalid_utf8_parquet_reading() {
        let parquet: Arc<str> = path_macro::path!(
            env!("CARGO_MANIFEST_DIR")
                / ".."
                / ".."
                / "tests"
                / "assets"
                / "parquet-data"
                / "invalid_utf8.parquet"
        )
        .to_str()
        .unwrap()
        .into();
        let io_config = IOConfig::default();
        let io_client = Arc::new(IOClient::new(io_config.into()).unwrap());
        let runtime_handle = get_io_runtime(true);
        let file_metadata = runtime_handle
            .block_on({
                let parquet = parquet.clone();
                let io_client = io_client.clone();
                async move { read_parquet_metadata(&parquet, io_client, None, None).await }
            })
            .flatten()
            .unwrap();
        let primitive_type = match file_metadata.schema_descr.fields() {
            [parquet_type] => match parquet_type {
                ParquetType::PrimitiveType(primitive_type) => primitive_type,
                ParquetType::GroupType { .. } => {
                    panic!("Parquet type should be primitive type, not group type")
                }
            },
            _ => panic!("This test parquet file should have only 1 field"),
        };
        assert_eq!(
            primitive_type.logical_type,
            Some(PrimitiveLogicalType::String)
        );
        assert_eq!(
            primitive_type.converted_type,
            Some(PrimitiveConvertedType::Utf8)
        );
        let (schema, _, _) = read_parquet_into_pyarrow(
            &parquet,
            None,
            None,
            None,
            None,
            io_client,
            None,
            true,
            ParquetSchemaInferenceOptions {
                string_encoding: StringEncoding::Raw,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        match schema.fields.as_slice() {
            [field] => assert_eq!(field.data_type, DataType::Binary),
            _ => panic!("There should only be one field in the schema"),
        };
    }
}
