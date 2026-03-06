use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use common_error::DaftResult;
use common_runtime::get_io_runtime;
use daft_core::prelude::*;
#[cfg(feature = "python")]
use daft_core::python::PyTimeUnit;
use daft_dsl::ExprRef;
use daft_io::{IOClient, IOStatsRef, SourceType, parse_url};
use daft_recordbatch::RecordBatch;
use futures::{
    StreamExt, TryFutureExt, TryStreamExt,
    future::{join_all, try_join_all},
    stream::BoxStream,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{DaftParquetMetadata, JoinSnafu, infer_schema_from_daft_metadata};

/// How to decode Parquet BYTE_ARRAY columns annotated as strings.
///
/// - `Utf8` (default): arrow-rs decodes as Utf8/LargeUtf8 with UTF-8 validation.
/// - `Raw`: strip the STRING logical type so arrow-rs decodes as Binary (no validation).
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StringEncoding {
    Raw,
    #[default]
    Utf8,
}

impl std::str::FromStr for StringEncoding {
    type Err = common_error::DaftError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "utf-8" => Ok(Self::Utf8),
            "raw" => Ok(Self::Raw),
            other => Err(common_error::DaftError::ValueError(format!(
                "Unrecognized string encoding: {other}"
            ))),
        }
    }
}

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
        let string_encoding: StringEncoding =
            value
                .string_encoding
                .parse()
                .map_err(|e: common_error::DaftError| crate::Error::ArrowError {
                    source: daft_arrow::error::Error::InvalidArgumentError(e.to_string()),
                })?;
        Ok(Self {
            coerce_int96_timestamp_unit: value
                .coerce_int96_timestamp_unit
                .map_or(TimeUnit::Nanoseconds, From::from),
            string_encoding,
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
    // TODO(arrow-rs): wire metadata through to the arrowrs reader to skip redundant footer reads.
    // The arrowrs reader currently reads its own metadata via ArrowReaderMetadata::load(),
    // but callers (e.g. scan_task.rs) already have pre-fetched DaftParquetMetadata from planning.
    _metadata: Option<Arc<DaftParquetMetadata>>,
    delete_rows: Option<Vec<i64>>,
    chunk_size: Option<usize>,
) -> DaftResult<RecordBatch> {
    let columns_ref: Option<Vec<&str>> = columns
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect());

    let (source_type, fixed_uri) = parse_url(uri)?;
    let table = if matches!(source_type, SourceType::File) {
        let (send, recv) = tokio::sync::oneshot::channel();
        let path = fixed_uri
            .strip_prefix("file://")
            .unwrap_or(&fixed_uri)
            .to_string();
        let columns_owned: Option<Vec<String>> = columns_ref
            .as_deref()
            .map(|c| c.iter().map(|s| (*s).to_string()).collect());
        let row_groups_owned = row_groups.as_deref().map(|r| r.to_vec());
        let predicate = predicate.clone();
        let field_id_mapping = field_id_mapping.clone();
        let delete_rows = delete_rows.clone();
        rayon::spawn(move || {
            let col_refs: Option<Vec<&str>> = columns_owned
                .as_ref()
                .map(|v| v.iter().map(|s| s.as_str()).collect());
            let result = crate::arrowrs_reader::local_parquet_read_arrowrs(
                &path,
                col_refs.as_deref(),
                start_offset,
                num_rows,
                row_groups_owned.as_deref(),
                predicate,
                schema_infer_options,
                chunk_size,
                field_id_mapping,
                delete_rows.as_deref(),
            );
            let _ = send.send(result);
        });
        recv.await.context(super::OneShotRecvSnafu {})??
    } else {
        crate::arrowrs_reader::read_parquet_single_arrowrs(
            uri,
            columns_ref.as_deref(),
            start_offset,
            num_rows,
            row_groups.as_deref(),
            predicate.clone(),
            io_client,
            io_stats,
            schema_infer_options,
            None,
            chunk_size,
            field_id_mapping,
            delete_rows.as_deref(),
        )
        .await?
    };
    Ok(table)
}

#[allow(clippy::too_many_arguments)]
async fn stream_parquet_single(
    uri: String,
    columns: Option<Vec<String>>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    // TODO(arrow-rs): wire metadata through to the arrowrs reader to skip redundant footer reads.
    _metadata: Option<Arc<DaftParquetMetadata>>,
    delete_rows: Option<Vec<i64>>,
    maintain_order: bool,
    chunk_size: Option<usize>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let columns_ref: Option<Vec<&str>> = columns
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect());

    let (source_type, fixed_uri) = parse_url(uri.as_str())?;
    let table_stream: BoxStream<'static, DaftResult<RecordBatch>> =
        if matches!(source_type, SourceType::File) {
            let path = fixed_uri
                .strip_prefix("file://")
                .unwrap_or(&fixed_uri)
                .to_string();
            crate::arrowrs_reader::local_parquet_stream_arrowrs(
                &path,
                columns_ref.as_deref(),
                num_rows,
                row_groups.as_deref(),
                predicate.clone(),
                schema_infer_options,
                chunk_size,
                field_id_mapping.clone(),
                delete_rows.as_deref(),
                maintain_order,
            )
            .await?
        } else {
            crate::arrowrs_reader::stream_parquet_single_arrowrs(
                uri.as_str(),
                columns_ref.as_deref(),
                None,
                num_rows,
                row_groups.as_deref(),
                predicate.clone(),
                io_client,
                io_stats,
                schema_infer_options,
                None,
                chunk_size,
                field_id_mapping,
                delete_rows.as_deref(),
            )
            .await?
        };

    let mut remaining_rows = num_rows.map(|limit| limit as i64);
    let stream = table_stream.try_take_while(move |table| {
        let should_continue = match remaining_rows {
            Some(rows_left) if rows_left <= 0 => false,
            Some(rows_left) => {
                remaining_rows = Some(rows_left - table.len() as i64);
                true
            }
            None => true,
        };
        futures::future::ready(Ok(should_continue))
    });
    Ok(stream.boxed())
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
) -> DaftResult<ParquetPyarrowChunk> {
    // Read data and metadata concurrently.
    // The metadata read is needed to recover schema-level key-value metadata
    // (e.g. custom metadata like {"str": "foo"}) that Daft's RecordBatch doesn't carry.
    let data_fut = read_parquet_single(
        uri,
        columns,
        start_offset,
        num_rows,
        row_groups,
        None, // predicate (pyarrow API doesn't expose it)
        io_client.clone(),
        io_stats.clone(),
        schema_infer_options,
        field_id_mapping,
        None, // metadata
        None, // delete_rows
        None, // chunk_size
    );
    let metadata_fut =
        crate::metadata::read_parquet_metadata(uri, None, io_client, io_stats, None, None);

    let (rb, parquet_metadata) =
        futures::future::try_join(data_fut, metadata_fut.err_into()).await?;
    let num_rows_read = rb.len();

    // Infer the Arrow schema from parquet metadata. This gives us:
    // - schema-level key-value metadata (handled the same way as pyarrow)
    // - per-field nullability from the parquet schema
    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        parquet_metadata.file_metadata().schema_descr(),
        parquet_metadata.file_metadata().key_value_metadata(),
    )
    .ok();
    let schema_metadata: std::collections::BTreeMap<String, String> = arrow_schema
        .as_ref()
        .map(|s| {
            s.metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .unwrap_or_default();

    // Convert each Daft Series → FFI-compatible arrays for the pyarrow bridge.
    // Return in COLUMN-MAJOR layout: all_arrays[col_idx] = [chunks_for_that_column].
    // The Python side (recordbatch.py) zips schema fields with this outer list,
    // so each entry must be the list of chunks for one column.
    let mut ffi_fields = Vec::with_capacity(rb.schema.fields().len());
    let mut all_arrays: Vec<ArrowChunk> = Vec::with_capacity(rb.schema.fields().len());

    for (col, daft_field) in rb.columns().iter().zip(rb.schema.fields()) {
        let arrow_array = col.to_arrow()?;
        let ffi_array = Box::<dyn daft_arrow::array::Array>::from(arrow_array);
        let nullable = arrow_schema
            .as_ref()
            .and_then(|s| s.field_with_name(&daft_field.name).ok())
            .is_none_or(|f| f.is_nullable());
        ffi_fields.push(daft_arrow::datatypes::Field::new(
            &daft_field.name,
            ffi_array.data_type().clone(),
            nullable,
        ));
        all_arrays.push(vec![ffi_array]);
    }

    let mut ffi_schema = daft_arrow::datatypes::Schema::from(ffi_fields);
    ffi_schema.metadata = schema_metadata;

    Ok((Arc::new(ffi_schema), all_arrays, num_rows_read))
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
    metadata: Option<Arc<DaftParquetMetadata>>,
) -> DaftResult<RecordBatch> {
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
pub type ArrowChunk = Vec<Box<dyn daft_arrow::array::Array>>;
pub type ArrowChunkIters = Vec<
    Box<
        dyn Iterator<Item = daft_arrow::error::Result<Box<dyn daft_arrow::array::Array>>>
            + Send
            + Sync,
    >,
>;
pub type ParquetPyarrowChunk = (daft_arrow::datatypes::SchemaRef, Vec<ArrowChunk>, usize);
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
    metadata: Option<Vec<Arc<DaftParquetMetadata>>>,
    delete_map: Option<HashMap<String, Vec<i64>>>,
    chunk_size: Option<usize>,
) -> DaftResult<Vec<RecordBatch>> {
    let runtime_handle = get_io_runtime(multithreaded_io);

    let columns = columns.map(|s| s.iter().map(|v| v.as_ref().to_string()).collect::<Vec<_>>());
    if let Some(ref row_groups) = row_groups
        && row_groups.len() != uris.len()
    {
        return Err(common_error::DaftError::ValueError(format!(
            "Mismatch of length of `uris` and `row_groups`. {} vs {}",
            uris.len(),
            row_groups.len()
        )));
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
    metadata: Option<Vec<Arc<DaftParquetMetadata>>>,
    delete_map: Option<HashMap<String, Vec<i64>>>,
    chunk_size: Option<usize>,
) -> DaftResult<Vec<DaftResult<RecordBatch>>> {
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
    columns: Option<Vec<String>>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: &ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    metadata: Option<Arc<DaftParquetMetadata>>,
    maintain_order: bool,
    delete_rows: Option<Vec<i64>>,
    chunk_size: Option<usize>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
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
        chunk_size,
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
    if let Some(ref row_groups) = row_groups
        && row_groups.len() != uris.len()
    {
        return Err(common_error::DaftError::ValueError(format!(
            "Mismatch of length of `uris` and `row_groups`. {} vs {}",
            uris.len(),
            row_groups.len()
        )));
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

pub async fn read_parquet_schema_and_metadata(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_inference_options: ParquetSchemaInferenceOptions,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<(Schema, DaftParquetMetadata)> {
    let metadata = crate::metadata::read_parquet_metadata(
        uri,
        None,
        io_client,
        io_stats,
        field_id_mapping,
        None,
    )
    .await?;
    let adapter = DaftParquetMetadata::from_arrowrs(metadata);
    let schema = infer_schema_from_daft_metadata(&adapter, schema_inference_options)?;
    Ok((schema, adapter))
}

pub async fn read_parquet_metadata(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<DaftParquetMetadata> {
    let metadata = crate::metadata::read_parquet_metadata(
        uri,
        None,
        io_client,
        io_stats,
        field_id_mapping,
        None,
    )
    .await?;
    Ok(DaftParquetMetadata::from_arrowrs(metadata))
}
pub async fn read_parquet_metadata_bulk(
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<Vec<DaftParquetMetadata>> {
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

/// Optimized for count pushdowns: we can get the count from metadata without reading all data.
pub async fn stream_parquet_count_pushdown(
    url: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    aggregation: &ExprRef,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let parquet_metadata =
        read_parquet_metadata(url, io_client, io_stats, field_id_mapping.clone()).await?;

    // Currently only CountMode::All is supported for count pushdown.
    let count = parquet_metadata.num_rows();
    let count_field = daft_core::datatypes::Field::new(
        aggregation.name(),
        daft_core::datatypes::DataType::UInt64,
    );
    let count_array =
        UInt64Array::from_iter(count_field.clone(), std::iter::once(Some(count as u64)));
    let count_batch = daft_recordbatch::RecordBatch::new_with_size(
        Schema::new(vec![count_field]),
        vec![count_array.into_series()],
        1,
    )?;
    Ok(Box::pin(futures::stream::once(
        async move { Ok(count_batch) },
    )))
}

pub fn read_parquet_statistics(
    uris: &Series,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
) -> DaftResult<RecordBatch> {
    let runtime_handle = get_io_runtime(true);

    if uris.data_type() != &DataType::Utf8 {
        return Err(common_error::DaftError::ValueError(format!(
            "Expected Utf8 Datatype, got {}",
            uris.data_type()
        )));
    }

    let path_array: &Utf8Array = uris.downcast()?;

    let handles_iter = path_array.into_iter().map(|uri| {
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
                let num_rows = metadata.num_rows();
                let num_row_groups = metadata.num_row_groups();
                let version_num = metadata.version();

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
        .zip(path_array.into_iter())
        .map(|(t, u)| {
            t.with_context(|_| JoinSnafu {
                path: u.unwrap().to_string(),
            })?
        })
        .collect::<DaftResult<Vec<_>>>()?;
    assert_eq!(all_tuples.len(), uris.len());

    let row_count_series = UInt64Array::from_iter(
        Field::new("row_count", DataType::UInt64),
        all_tuples.iter().map(|v| v.0.map(|v| v as u64)),
    );
    let row_group_series = UInt64Array::from_iter(
        Field::new("row_group_count", DataType::UInt64),
        all_tuples.iter().map(|v| v.1.map(|v| v as u64)),
    );
    let version_series = Int32Array::from_iter(
        Field::new("version", DataType::Int32),
        all_tuples.iter().map(|v| v.2),
    );

    RecordBatch::from_nonempty_columns(vec![
        uris.clone(),
        row_count_series.into_series(),
        row_group_series.into_series(),
        version_series.into_series(),
    ])
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use common_error::DaftResult;
    use daft_arrow::datatypes::DataType;
    use daft_io::{IOClient, IOConfig};
    use futures::StreamExt;
    use parquet::schema::types::Type as ParquetSchemaType;

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

        runtime_handle.block_within_async_context(async move {
            let metadata = read_parquet_metadata(&file, io_client, None, None).await?;
            let config = bincode::config::legacy();
            let serialized = bincode::serde::encode_to_vec(&metadata, config).unwrap();
            let deserialized: DaftParquetMetadata =
                bincode::serde::decode_from_slice(&serialized, config)
                    .unwrap()
                    .0;
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
            .block_within_async_context({
                let parquet = parquet.clone();
                let io_client = io_client.clone();
                async move { read_parquet_metadata(&parquet, io_client, None, None).await }
            })
            .flatten()
            .unwrap();
        let schema_descr = file_metadata.schema_descriptor();
        let fields = schema_descr.root_schema().get_fields();
        assert_eq!(
            fields.len(),
            1,
            "This test parquet file should have only 1 field"
        );
        match fields[0].as_ref() {
            ParquetSchemaType::PrimitiveType { basic_info, .. } => {
                assert_eq!(
                    basic_info.logical_type_ref(),
                    Some(&parquet::basic::LogicalType::String),
                );
                assert_eq!(
                    basic_info.converted_type(),
                    parquet::basic::ConvertedType::UTF8,
                );
            }
            ParquetSchemaType::GroupType { .. } => {
                panic!("Parquet type should be primitive type, not group type")
            }
        }
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
            [field] => assert_eq!(field.data_type, DataType::LargeBinary),
            _ => panic!("There should only be one field in the schema"),
        };
    }

    /// Regression test: streaming with a limit equal to the batch size should
    /// return all requested rows, not an empty stream.
    #[test]
    fn test_stream_limit_exact_batch_size() {
        use arrow::{
            array::Int32Array,
            datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        };
        use parquet::arrow::ArrowWriter;

        let dir = std::env::temp_dir().join("daft_test_stream_limit_exact");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        // Write a parquet file with exactly 5 rows.
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap().to_string();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());
        let runtime = get_io_runtime(true);

        // Stream with num_rows=5 (exactly the file size).
        let total_rows: usize = runtime
            .block_within_async_context(async move {
                let mut stream = stream_parquet_single(
                    uri,
                    None,
                    Some(5),
                    None,
                    None,
                    io_client,
                    None,
                    ParquetSchemaInferenceOptions::default(),
                    None,
                    None,
                    None,
                    true,
                    None,
                )
                .await
                .unwrap();

                let mut count = 0;
                while let Some(batch) = stream.next().await {
                    count += batch.unwrap().len();
                }
                count
            })
            .unwrap();

        assert_eq!(
            total_rows, 5,
            "stream with limit=5 on 5-row file should return 5 rows"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
