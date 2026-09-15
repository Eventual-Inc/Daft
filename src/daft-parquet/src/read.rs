use std::{collections::BTreeMap, sync::Arc, time::Duration};

use arrow::array::ArrayRef;
use common_error::DaftResult;
use common_runtime::{OrderedJoinSet, get_io_runtime};
use daft_core::prelude::*;
#[cfg(feature = "python")]
use daft_core::python::PyTimeUnit;
use daft_dsl::ExprRef;
use daft_io::{IOClient, IOStatsRef, SourceType, parse_url};
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, TryFutureExt, TryStreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize};

use crate::{DaftParquetMetadata, infer_schema_from_daft_metadata};

/// (path, reason, partial). `partial=true` means some batches were already emitted before
/// corruption was detected; the file is not fully skipped.
type SkippedCorruptFilesCollector =
    Option<std::sync::Arc<std::sync::Mutex<Vec<(String, String, bool)>>>>;

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ParquetSchemaInferenceOptions {
    pub coerce_int96_timestamp_unit: TimeUnit,
    pub string_encoding: StringEncoding,
}

impl ParquetSchemaInferenceOptions {
    #[must_use]
    pub fn new(coerce_int96_timestamp_unit: Option<TimeUnit>) -> Self {
        Self {
            coerce_int96_timestamp_unit: coerce_int96_timestamp_unit
                .unwrap_or(TimeUnit::Nanoseconds),
            string_encoding: StringEncoding::Utf8,
        }
    }

    #[cfg(feature = "python")]
    pub fn from_python(
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
        string_encoding: &str,
    ) -> DaftResult<Self> {
        Ok(Self {
            coerce_int96_timestamp_unit: coerce_int96_timestamp_unit
                .map_or(TimeUnit::Nanoseconds, From::from),
            string_encoding: string_encoding.parse()?,
        })
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

/// All projection, pushdown, and decode options for reading one parquet file.
#[derive(Default, Clone)]
pub struct ParquetReadOptions {
    pub columns: Option<Vec<String>>,
    pub start_offset: Option<usize>,
    pub num_rows: Option<usize>,
    pub row_groups: Option<Vec<i64>>,
    pub predicate: Option<ExprRef>,
    pub schema_infer: ParquetSchemaInferenceOptions,
    pub field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    pub delete_rows: Option<Vec<i64>>,
    pub batch_size: Option<usize>,
    // TODO(arrow-rs): wire this through to the arrowrs reader to skip redundant footer reads.
    // The arrowrs reader currently reads its own metadata via ArrowReaderMetadata::load(),
    // but callers (e.g. scan_task.rs) already have pre-fetched DaftParquetMetadata from planning.
    pub metadata: Option<Arc<DaftParquetMetadata>>,
    pub ignore_corrupt_files: bool,
    pub skipped_corrupt_files: SkippedCorruptFilesCollector,
}

/// Per-file overrides for [`ParquetBulkReadOptions`].
#[derive(Default, Clone)]
pub struct PerFileOptions {
    pub row_groups: Option<Vec<i64>>,
    pub delete_rows: Option<Vec<i64>>,
    /// See [`ParquetReadOptions::metadata`].
    pub metadata: Option<Arc<DaftParquetMetadata>>,
}

/// Options for bulk reads. Fields without `per_file` apply to every uri;
/// `per_file[i]` overrides for the i-th uri.
#[derive(Default, Clone)]
pub struct ParquetBulkReadOptions {
    pub columns: Option<Vec<String>>,
    pub start_offset: Option<usize>,
    pub num_rows: Option<usize>,
    pub predicate: Option<ExprRef>,
    pub schema_infer: ParquetSchemaInferenceOptions,
    pub field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    pub batch_size: Option<usize>,
    pub num_parallel_tasks: usize,
    /// Per-uri overrides. Must be empty or `len() == uris.len()`.
    pub per_file: Vec<PerFileOptions>,
}

fn make_source<'a>(
    uri: &'a str,
    local_path: &'a mut String,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<crate::reader::ParquetSource<'a>> {
    let (source_type, fixed_uri) = parse_url(uri)?;
    Ok(if matches!(source_type, SourceType::File) {
        *local_path = daft_io::strip_file_uri_to_path(&fixed_uri)
            .unwrap_or(&fixed_uri)
            .to_string();
        crate::reader::ParquetSource::Local { path: local_path }
    } else {
        crate::reader::ParquetSource::Url {
            uri,
            io_client,
            io_stats,
        }
    })
}

fn check_per_file_len(per_file: &[PerFileOptions], uris_len: usize) -> DaftResult<()> {
    if !per_file.is_empty() && per_file.len() != uris_len {
        return Err(common_error::DaftError::ValueError(format!(
            "Mismatch of length of `uris` and `per_file`. {} vs {}",
            uris_len,
            per_file.len()
        )));
    }
    Ok(())
}

fn single_opts_for(opts: &ParquetBulkReadOptions, i: usize) -> ParquetReadOptions {
    let per = opts.per_file.get(i).cloned().unwrap_or_default();
    ParquetReadOptions {
        columns: opts.columns.clone(),
        start_offset: opts.start_offset,
        num_rows: opts.num_rows,
        row_groups: per.row_groups,
        predicate: opts.predicate.clone(),
        schema_infer: opts.schema_infer,
        field_id_mapping: opts.field_id_mapping.clone(),
        delete_rows: per.delete_rows,
        batch_size: opts.batch_size,
        metadata: per.metadata,
        ignore_corrupt_files: false,
        skipped_corrupt_files: None,
    }
}

/// Read a single parquet file as a stream of `RecordBatch`es.
pub async fn read_parquet(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetReadOptions,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let ignore_corrupt_files = opts.ignore_corrupt_files;
    let skipped_corrupt_files = opts.skipped_corrupt_files.clone();
    let uri_owned = uri.to_string();
    let mut local_path = String::new();
    let source = make_source(uri, &mut local_path, io_client, io_stats)?;
    match Box::pin(crate::reader::stream_parquet(source, &opts)).await {
        Ok((_schema, stream)) => {
            if ignore_corrupt_files {
                let uri_for_warn = uri_owned.clone();
                let collector = skipped_corrupt_files.clone();
                let saw_ok = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let filtered = stream.filter_map(move |result| {
                    let uri_w = uri_for_warn.clone();
                    let coll = collector.clone();
                    let saw = saw_ok.clone();
                    futures::future::ready(match result {
                        Ok(batch) => {
                            saw.store(true, std::sync::atomic::Ordering::Relaxed);
                            Some(Ok(batch))
                        }
                        Err(ref e) if is_parquet_corrupt(e) => {
                            let partial = saw.load(std::sync::atomic::Ordering::Relaxed);
                            log::warn!(
                                "Skipping corrupt row-group data in Parquet file {uri_w} (partial={partial}): {e}"
                            );
                            if let Some(ref c) = coll
                                && let Ok(mut v) = c.lock()
                            {
                                v.push((uri_w, e.to_string(), partial));
                            }
                            None
                        }
                        Err(e) => Some(Err(e)),
                    })
                });
                Ok(Box::pin(filtered))
            } else {
                Ok(stream)
            }
        }
        Err(ref e) if ignore_corrupt_files && is_parquet_corrupt(e) => {
            log::warn!("Skipping corrupt Parquet file {uri_owned}: {e}");
            if let Some(ref c) = skipped_corrupt_files
                && let Ok(mut v) = c.lock()
            {
                v.push((uri_owned, e.to_string(), false));
            }
            Ok(Box::pin(futures::stream::empty()))
        }
        Err(e) => Err(e),
    }
}

/// Eager variant of `read_parquet`: collects the full stream into one
/// `RecordBatch`, or a schema-bearing empty batch if the stream produced none.
pub async fn read_parquet_into_recordbatch(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetReadOptions,
) -> DaftResult<RecordBatch> {
    let mut local_path = String::new();
    let source = make_source(uri, &mut local_path, io_client, io_stats)?;
    let (schema, stream) = Box::pin(crate::reader::stream_parquet(source, &opts)).await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    if batches.is_empty() {
        return Ok(RecordBatch::empty(Some(schema)));
    }
    RecordBatch::concat(&batches)
}

/// Read a single parquet file and convert to pyarrow-friendly `ArrowChunk`s,
/// preserving file-level kv metadata + per-field nullability from the parquet
/// schema (info that the daft `RecordBatch` path drops).
async fn read_parquet_into_arrow(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetReadOptions,
) -> DaftResult<ParquetPyarrowChunk> {
    debug_assert!(
        opts.predicate.is_none() && opts.delete_rows.is_none() && opts.batch_size.is_none(),
        "read_parquet_into_arrow: predicate/delete_rows/batch_size not supported on the pyarrow path"
    );

    // Read data and metadata concurrently. The metadata read is needed to recover
    // schema-level key-value metadata (e.g. custom metadata like {"str": "foo"}) and
    // per-field nullability — info that Daft's `RecordBatch` doesn't carry.
    let data_fut = read_parquet_into_recordbatch(uri, io_client.clone(), io_stats.clone(), opts);
    let metadata_fut =
        crate::metadata::read_parquet_metadata(uri, None, io_client, io_stats, None, None);
    let (rb, parquet_metadata) =
        Box::pin(futures::future::try_join(data_fut, metadata_fut.err_into())).await?;
    let num_rows_read = rb.len();

    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        parquet_metadata.file_metadata().schema_descr(),
        parquet_metadata.file_metadata().key_value_metadata(),
    )
    .ok();
    let schema_metadata = arrow_schema
        .as_ref()
        .map(|s| s.metadata().clone())
        .unwrap_or_default();

    // Convert each Daft Series → FFI-compatible arrays for the pyarrow bridge.
    // Output layout is COLUMN-MAJOR: all_arrays[col_idx] = [chunks_for_that_column].
    // The Python side (recordbatch.py) zips schema fields with this outer list,
    // so each entry must be the list of chunks for one column.
    let mut ffi_fields = Vec::with_capacity(rb.schema.fields().len());
    let mut all_arrays: Vec<ArrowChunk> = Vec::with_capacity(rb.schema.fields().len());
    for (col, daft_field) in rb.columns().iter().zip(rb.schema.fields()) {
        let arrow_array = col.as_materialized_series().to_arrow()?;
        let nullable = arrow_schema
            .as_ref()
            .and_then(|s| s.field_with_name(&daft_field.name).ok())
            .is_none_or(|f| f.is_nullable());
        ffi_fields.push(arrow::datatypes::Field::new(
            daft_field.name.to_string(),
            arrow_array.data_type().clone(),
            nullable,
        ));
        all_arrays.push(vec![arrow_array]);
    }

    let mut ffi_schema = arrow::datatypes::Schema::new(ffi_fields);
    ffi_schema.metadata = schema_metadata;
    Ok((Arc::new(ffi_schema), all_arrays, num_rows_read))
}

pub type ArrowChunk = Vec<ArrayRef>;
pub type ParquetPyarrowChunk = (arrow::datatypes::SchemaRef, Vec<ArrowChunk>, usize);

pub fn read_parquet_into_pyarrow(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    opts: ParquetReadOptions,
    file_timeout_ms: Option<i64>,
) -> DaftResult<ParquetPyarrowChunk> {
    get_io_runtime(multithreaded_io).block_on_current_thread(async {
        let fut = Box::pin(read_parquet_into_arrow(uri, io_client, io_stats, opts));
        match file_timeout_ms {
            Some(timeout) => tokio::time::timeout(Duration::from_millis(timeout as u64), fut)
                .await
                .map_err(|_| crate::Error::FileReadTimeout {
                    path: uri.to_string(),
                    duration_ms: timeout,
                })?,
            None => fut.await,
        }
    })
}

pub async fn read_parquet_bulk(
    uris: Vec<String>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetBulkReadOptions,
) -> DaftResult<Vec<DaftResult<RecordBatch>>> {
    check_per_file_len(&opts.per_file, uris.len())?;

    let num_parallel = opts.num_parallel_tasks.max(1);
    let io_runtime = get_io_runtime(true);
    let task_stream = futures::stream::iter(uris.into_iter().enumerate().map(|(i, uri)| {
        let single_opts = single_opts_for(&opts, i);
        let io_client = io_client.clone();
        let io_stats = io_stats.clone();
        io_runtime.spawn(async move {
            Box::pin(read_parquet_into_recordbatch(
                &uri,
                io_client,
                io_stats,
                single_opts,
            ))
            .await
        })
    }));

    let mut remaining_rows = opts.num_rows.map(|x| x as i64);
    let tables = task_stream
        .buffered(num_parallel)
        .try_take_while(|result| match (result, remaining_rows) {
            (_, Some(rows_left)) if rows_left <= 0 => futures::future::ready(Ok(false)),
            (Ok(table), Some(rows_left)) => {
                remaining_rows = Some(rows_left - table.len() as i64);
                futures::future::ready(Ok(true))
            }
            (_, None) | (Err(_), _) => futures::future::ready(Ok(true)),
        })
        .try_collect::<Vec<_>>()
        .await?;
    Ok(tables)
}

pub fn read_parquet_bulk_sync(
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    opts: ParquetBulkReadOptions,
) -> DaftResult<Vec<RecordBatch>> {
    let uris_owned: Vec<String> = uris.iter().map(|s| (*s).to_string()).collect();
    get_io_runtime(multithreaded_io)
        .block_on_current_thread(read_parquet_bulk(uris_owned, io_client, io_stats, opts))?
        .into_iter()
        .collect()
}

/// Returns true if the error represents genuine Parquet file corruption.
///
/// Bad magic bytes, truncated footer, and corrupt row-group data return true.
/// Network errors, permission errors, and other transient failures return false.
pub fn is_parquet_corrupt(err: &common_error::DaftError) -> bool {
    use common_error::DaftError;
    match err {
        DaftError::CorruptFile(_) => true,
        DaftError::IoError(io_err) => {
            matches!(io_err.kind(), std::io::ErrorKind::UnexpectedEof)
        }
        DaftError::FileNotFound { .. } => true,
        _ => false,
    }
}

pub fn read_parquet_into_pyarrow_bulk(
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    opts: ParquetBulkReadOptions,
) -> DaftResult<Vec<ParquetPyarrowChunk>> {
    check_per_file_len(&opts.per_file, uris.len())?;
    let num_parallel = opts.num_parallel_tasks.max(1);
    let io_runtime = get_io_runtime(multithreaded_io);
    let spawn_runtime = io_runtime.clone();
    let results = io_runtime.block_on_current_thread(async move {
        futures::stream::iter(uris.iter().enumerate().map(|(i, uri)| {
            let uri = (*uri).to_string();
            let single_opts = single_opts_for(&opts, i);
            let io_client = io_client.clone();
            let io_stats = io_stats.clone();
            spawn_runtime.spawn(async move {
                Ok((
                    i,
                    Box::pin(read_parquet_into_arrow(
                        &uri,
                        io_client,
                        io_stats,
                        single_opts,
                    ))
                    .await?,
                ))
            })
        }))
        .buffer_unordered(num_parallel)
        .try_collect::<Vec<_>>()
        .await
    })?;
    let mut collected = results.into_iter().collect::<DaftResult<Vec<_>>>()?;
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
    let io_runtime = get_io_runtime(true);
    let mut joinset: OrderedJoinSet<DaftResult<DaftParquetMetadata>> = OrderedJoinSet::new();
    for uri in uris {
        let uri = (*uri).to_string();
        let io_client = io_client.clone();
        let io_stats = io_stats.clone();
        let field_id_mapping = field_id_mapping.clone();
        joinset.spawn_on(
            async move { read_parquet_metadata(&uri, io_client, io_stats, field_id_mapping).await },
            &io_runtime,
        );
    }
    let mut results = Vec::with_capacity(uris.len());
    while let Some(res) = joinset.join_next().await {
        results.push(res??);
    }
    Ok(results)
}

/// Optimized for count pushdowns: get the row count from metadata without reading data.
///
/// This path is only used when `ignore_corrupt_files` is false (the optimizer gates
/// count pushdown via `supports_count_pushdown`), so metadata errors propagate normally.
pub async fn stream_parquet_count_pushdown(
    url: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    aggregation: &ExprRef,
    row_groups: Option<&[i64]>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let parquet_metadata =
        read_parquet_metadata(url, io_client, io_stats, field_id_mapping.clone()).await?;

    // Currently only CountMode::All is supported for count pushdown.
    //
    // When the ScanTask constrains the read to specific row groups, count only those.
    // Indices are positional into the full footer, mirroring `prune_row_groups`.
    let count = match row_groups {
        None => parquet_metadata.num_rows(),
        Some(rgs) => {
            let arrow_md = parquet_metadata.as_arrowrs();
            let indices = crate::helpers::validate_requested_row_groups(arrow_md, rgs, url)?;
            indices
                .iter()
                .map(|&i| arrow_md.row_group(i).num_rows() as usize)
                .sum()
        }
    };
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
    if uris.data_type() != &DataType::Utf8 {
        return Err(common_error::DaftError::ValueError(format!(
            "Expected Utf8 Datatype, got {}",
            uris.data_type()
        )));
    }
    let path_array: &Utf8Array = uris.downcast()?;

    type StatsTuple = (Option<usize>, Option<usize>, Option<i32>);
    let runtime = get_io_runtime(true);
    let spawn_runtime = runtime.clone();
    let all = runtime.block_on_current_thread(async move {
        let mut joinset: OrderedJoinSet<DaftResult<StatsTuple>> = OrderedJoinSet::new();
        for uri in path_array {
            let uri = uri.map(std::string::ToString::to_string);
            let io_client = io_client.clone();
            let io_stats = io_stats.clone();
            let field_id_mapping = field_id_mapping.clone();
            joinset.spawn_on(
                async move {
                    match uri {
                        Some(uri) => {
                            let m =
                                read_parquet_metadata(&uri, io_client, io_stats, field_id_mapping)
                                    .await?;
                            Ok((
                                Some(m.num_rows()),
                                Some(m.num_row_groups()),
                                Some(m.version()),
                            ))
                        }
                        None => Ok((None, None, None)),
                    }
                },
                &spawn_runtime,
            );
        }
        let mut out = Vec::with_capacity(uris.len());
        while let Some(res) = joinset.join_next().await {
            out.push(res??);
        }
        DaftResult::Ok(out)
    })?;
    assert_eq!(all.len(), uris.len());

    let rows = UInt64Array::from_iter(
        Field::new("row_count", DataType::UInt64),
        all.iter().map(|v| v.0.map(|v| v as u64)),
    );
    let rgs = UInt64Array::from_iter(
        Field::new("row_group_count", DataType::UInt64),
        all.iter().map(|v| v.1.map(|v| v as u64)),
    );
    let versions = Int32Array::from_iter(
        Field::new("version", DataType::Int32),
        all.iter().map(|v| v.2),
    );

    RecordBatch::from_nonempty_columns(vec![
        uris.clone(),
        rows.into_series(),
        rgs.into_series(),
        versions.into_series(),
    ])
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, path::PathBuf, sync::Arc};

    use arrow::datatypes::DataType;
    use common_error::DaftResult;
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
        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let runtime = get_io_runtime(true);
        let table = runtime.block_on_current_thread(read_parquet_into_recordbatch(
            PARQUET_FILE,
            io_client,
            None,
            ParquetReadOptions::default(),
        ))?;
        assert_eq!(table.len(), 100);
        Ok(())
    }

    #[test]
    fn test_parquet_streaming_read_from_s3() -> DaftResult<()> {
        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let runtime = get_io_runtime(true);
        runtime.block_on_current_thread(async move {
            let stream =
                read_parquet(PARQUET_FILE, io_client, None, ParquetReadOptions::default()).await?;
            let tables = stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<DaftResult<Vec<_>>>()?;
            let total = tables.iter().map(|t| t.len()).sum::<usize>();
            assert_eq!(total, 100);
            Ok(())
        })
    }

    #[test]
    fn test_file_metadata_serialize_roundtrip() -> DaftResult<()> {
        let file = get_local_parquet_path();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into())?);
        let runtime = get_io_runtime(true);

        runtime.block_within_async_context(async move {
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
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());
        let runtime = get_io_runtime(true);
        let file_metadata = runtime
            .block_within_async_context({
                let parquet = parquet.clone();
                let io_client = io_client.clone();
                async move { read_parquet_metadata(&parquet, io_client, None, None).await }
            })
            .flatten()
            .unwrap();
        let schema_descr = file_metadata.schema_descriptor();
        let fields = schema_descr.root_schema().get_fields();
        assert_eq!(fields.len(), 1);
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
            ParquetSchemaType::GroupType { .. } => panic!("primitive expected"),
        }
        let opts = ParquetReadOptions {
            schema_infer: ParquetSchemaInferenceOptions {
                string_encoding: StringEncoding::Raw,
                ..Default::default()
            },
            ..Default::default()
        };
        let (schema, _, _) =
            read_parquet_into_pyarrow(&parquet, io_client, None, true, opts, None).unwrap();
        match schema.fields().deref() {
            [field] => assert_eq!(field.data_type(), &DataType::LargeBinary),
            _ => panic!("one field expected"),
        }
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
                let opts = ParquetReadOptions {
                    num_rows: Some(5),
                    ..Default::default()
                };
                let mut stream = read_parquet(&uri, io_client, None, opts).await.unwrap();
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

    // ---------------------------------------------------------------------
    // LM-pipelined predicate evaluation (#6340)
    //
    // These tests drive the full read path with real multi-row-group Parquet
    // files and assert exact output rows/values. The pipelined path
    // (`a > x AND b < y`, column-disjoint) and the monolithic fallbacks
    // (single column, or same-column conjunction) must all produce identical,
    // correctly-ordered results.
    // ---------------------------------------------------------------------

    /// Deterministic row generator: `a = i` (nullable every 7th row when
    /// `a_nulls`), `b = i % 50`, `c = i * 10`. Shared by the writer and the
    /// expected-value computation so they can never drift.
    fn pipelined_test_rows(n: usize, a_nulls: bool) -> Vec<(Option<i64>, i64, i64)> {
        (0..n)
            .map(|i| {
                let iv = i as i64;
                let a = if a_nulls && iv % 7 == 0 {
                    None
                } else {
                    Some(iv)
                };
                (a, iv % 50, iv * 10)
            })
            .collect()
    }

    fn write_pipelined_test_parquet(
        path: &std::path::Path,
        n: usize,
        rg_size: usize,
        a_nulls: bool,
    ) {
        use arrow::{
            array::{Int64Array, RecordBatch as ArrowRecordBatch},
            datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        };
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int64, true),
            ArrowField::new("b", ArrowDataType::Int64, false),
            ArrowField::new("c", ArrowDataType::Int64, false),
        ]));
        let rows = pipelined_test_rows(n, a_nulls);
        let a: Int64Array = rows.iter().map(|(a, _, _)| *a).collect();
        let b: Int64Array = rows.iter().map(|(_, b, _)| *b).collect();
        let c: Int64Array = rows.iter().map(|(_, _, c)| *c).collect();
        let batch =
            ArrowRecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b), Arc::new(c)]).unwrap();
        write_pipelined_test_batch(path, &batch, rg_size);
    }

    fn write_pipelined_test_batch(
        path: &std::path::Path,
        batch: &arrow::array::RecordBatch,
        rg_size: usize,
    ) {
        use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rg_size))
            .build();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        for offset in (0..batch.num_rows()).step_by(rg_size) {
            writer
                .write(&batch.slice(offset, rg_size.min(batch.num_rows() - offset)))
                .unwrap();
        }
        writer.close().unwrap();
    }

    fn read_pipelined(
        path: &str,
        predicate: Option<daft_dsl::ExprRef>,
        columns: Vec<String>,
    ) -> daft_recordbatch::RecordBatch {
        read_pipelined_with_limit(path, predicate, columns, None)
    }

    fn read_pipelined_with_limit(
        path: &str,
        predicate: Option<daft_dsl::ExprRef>,
        columns: Vec<String>,
        num_rows: Option<usize>,
    ) -> daft_recordbatch::RecordBatch {
        let path = path.to_string();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());
        let runtime = get_io_runtime(true);
        runtime
            .block_within_async_context(async move {
                let opts = ParquetReadOptions {
                    predicate,
                    columns: Some(columns),
                    num_rows,
                    ..Default::default()
                };
                read_parquet_into_recordbatch(&path, io_client, None, opts)
                    .await
                    .unwrap()
            })
            .unwrap()
    }

    fn pipelined_col_i64(batch: &daft_recordbatch::RecordBatch, name: &str) -> Vec<Option<i64>> {
        let idx = batch
            .schema
            .get_fields_with_name(name)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("column {name} missing from output"))
            .0;
        let arr = batch.get_column(idx).i64().unwrap();
        (0..arr.len()).map(|i| arr.get(i)).collect()
    }

    #[test]
    fn test_pipelined_compound_disjoint_predicate() {
        use daft_dsl::{lit, resolved_col};

        let dir = std::env::temp_dir().join("daft_test_pipelined_compound");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.parquet");
        // 200 rows across ~4 row groups of 64.
        write_pipelined_test_parquet(&path, 200, 64, false);
        let uri = path.to_str().unwrap().to_string();

        // a > 100 AND b < 10  →  column-disjoint  →  pipelined path.
        let pred = resolved_col("a")
            .gt(lit(100))
            .and(resolved_col("b").lt(lit(10)));
        crate::reader::assert_prefilter_strategy(&pred, None, true);
        let batch = read_pipelined(&uri, Some(pred), vec!["a".into(), "b".into(), "c".into()]);

        let expected: Vec<(Option<i64>, i64, i64)> = pipelined_test_rows(200, false)
            .into_iter()
            .filter(|(a, b, _)| a.is_some_and(|av| av > 100) && *b < 10)
            .collect();
        assert!(
            expected.len() > 1 && expected.len() < 200,
            "test predicate should be selective but non-empty, got {}",
            expected.len()
        );

        assert_eq!(batch.len(), expected.len(), "row count mismatch");
        assert_eq!(
            pipelined_col_i64(&batch, "a"),
            expected.iter().map(|(a, _, _)| *a).collect::<Vec<_>>()
        );
        assert_eq!(
            pipelined_col_i64(&batch, "b"),
            expected
                .iter()
                .map(|(_, b, _)| Some(*b))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            pipelined_col_i64(&batch, "c"),
            expected
                .iter()
                .map(|(_, _, c)| Some(*c))
                .collect::<Vec<_>>()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_pipelined_matches_monolithic_same_data() {
        use daft_dsl::{lit, resolved_col};

        let dir =
            std::env::temp_dir().join(format!("daft_test_pipelined_vs_mono_{}", fastrand::u64(..)));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.parquet");
        let total_fixture_rows = 200;
        write_pipelined_test_parquet(&path, total_fixture_rows, 64, true);
        let uri = path.to_str().unwrap().to_string();

        let pred = resolved_col("a")
            .gt(lit(100))
            .and(resolved_col("b").lt(lit(10)));
        crate::reader::assert_prefilter_strategy(&pred, None, true);
        crate::reader::assert_prefilter_strategy(&pred, Some(total_fixture_rows), false);
        let columns = vec!["a".into(), "b".into(), "c".into()];
        let pipelined = read_pipelined(&uri, Some(pred.clone()), columns.clone());
        let mono = read_pipelined_with_limit(&uri, Some(pred), columns, Some(total_fixture_rows));

        assert!(!pipelined.is_empty() && pipelined.len() < total_fixture_rows);
        assert_eq!(pipelined.len(), mono.len());
        for name in ["a", "b", "c"] {
            assert_eq!(
                pipelined_col_i64(&pipelined, name),
                pipelined_col_i64(&mono, name)
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_monolithic_fallback_column_dependent_is_in() {
        use arrow::{
            array::{Int64Array, RecordBatch as ArrowRecordBatch},
            datatypes::{Field as ArrowField, Schema as ArrowSchema},
        };
        use daft_dsl::{lit, resolved_col};

        let dir =
            std::env::temp_dir().join(format!("daft_test_pipelined_is_in_{}", fastrand::u64(..)));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.parquet");
        let schema = Arc::new(ArrowSchema::new(
            ["a", "b", "c", "d"]
                .map(|name| ArrowField::new(name, DataType::Int64, false))
                .to_vec(),
        ));
        let batch = ArrowRecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1])),
                Arc::new(Int64Array::from(vec![9, 5])),
                Arc::new(Int64Array::from(vec![5, 9])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        write_pipelined_test_batch(&path, &batch, 2);
        let pred = resolved_col("a")
            .eq(lit(1))
            .and(resolved_col("b").is_in(vec![resolved_col("c")]));
        let result = read_pipelined(path.to_str().unwrap(), Some(pred.clone()), vec!["d".into()]);

        assert_eq!(pipelined_col_i64(&result, "d"), vec![Some(20)]);
        assert_eq!(result.schema.field_names().collect::<Vec<_>>(), vec!["d"]);
        crate::reader::assert_prefilter_strategy(&pred, None, false);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_monolithic_fallback_constant_conjuncts() {
        use daft_dsl::{lit, null_lit, resolved_col};

        let dir = std::env::temp_dir().join(format!(
            "daft_test_pipelined_constants_{}",
            fastrand::u64(..)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.parquet");
        write_pipelined_test_parquet(&path, 20, 20, true);
        let expected: Vec<_> = pipelined_test_rows(20, true)
            .into_iter()
            .filter(|(a, b, _)| a.is_some_and(|a| a > 0) && *b < 10)
            .map(|(_, _, c)| Some(c))
            .collect();
        assert!(!expected.is_empty());

        for (constant, keeps_rows) in [
            (lit(true), true),
            (lit(false), false),
            (null_lit(), false),
            (
                null_lit().cast(&daft_core::prelude::DataType::Boolean),
                false,
            ),
        ] {
            let pred = resolved_col("a")
                .gt(lit(0))
                .and(resolved_col("b").lt(lit(10)))
                .and(constant);
            let result =
                read_pipelined(path.to_str().unwrap(), Some(pred.clone()), vec!["c".into()]);
            assert_eq!(
                pipelined_col_i64(&result, "c"),
                if keeps_rows {
                    expected.clone()
                } else {
                    Vec::new()
                },
                "predicate: {pred}"
            );
            crate::reader::assert_prefilter_strategy(&pred, None, false);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_monolithic_fallback_single_column() {
        use daft_dsl::{lit, resolved_col};

        let dir = std::env::temp_dir().join("daft_test_pipelined_mono_single");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.parquet");
        write_pipelined_test_parquet(&path, 200, 64, false);
        let uri = path.to_str().unwrap().to_string();

        // Single-column predicate → one group → monolithic fallback.
        let pred = resolved_col("a").gt(lit(100));
        let batch = read_pipelined(&uri, Some(pred), vec!["a".into(), "c".into()]);

        let expected: Vec<(Option<i64>, i64)> = pipelined_test_rows(200, false)
            .into_iter()
            .filter(|(a, _, _)| a.is_some_and(|av| av > 100))
            .map(|(a, _, c)| (a, c))
            .collect();
        assert_eq!(batch.len(), expected.len());
        assert_eq!(
            pipelined_col_i64(&batch, "a"),
            expected.iter().map(|(a, _)| *a).collect::<Vec<_>>()
        );
        assert_eq!(
            pipelined_col_i64(&batch, "c"),
            expected.iter().map(|(_, c)| Some(*c)).collect::<Vec<_>>()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_monolithic_fallback_same_column_conjunction() {
        use daft_dsl::{lit, resolved_col};

        let dir = std::env::temp_dir().join("daft_test_pipelined_mono_samecol");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.parquet");
        write_pipelined_test_parquet(&path, 200, 64, false);
        let uri = path.to_str().unwrap().to_string();

        // a > 100 AND a < 150 → same column → merged into one group → monolithic.
        let pred = resolved_col("a")
            .gt(lit(100))
            .and(resolved_col("a").lt(lit(150)));
        let batch = read_pipelined(&uri, Some(pred), vec!["a".into(), "c".into()]);

        let expected: Vec<(Option<i64>, i64)> = pipelined_test_rows(200, false)
            .into_iter()
            .filter(|(a, _, _)| a.is_some_and(|av| av > 100 && av < 150))
            .map(|(a, _, c)| (a, c))
            .collect();
        assert_eq!(batch.len(), expected.len());
        assert_eq!(
            pipelined_col_i64(&batch, "a"),
            expected.iter().map(|(a, _)| *a).collect::<Vec<_>>()
        );
        assert_eq!(
            pipelined_col_i64(&batch, "c"),
            expected.iter().map(|(_, c)| Some(*c)).collect::<Vec<_>>()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_pipelined_null_handling() {
        use daft_dsl::{lit, resolved_col};

        let dir = std::env::temp_dir().join("daft_test_pipelined_nulls");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.parquet");
        // `a` is null on every 7th row; those rows must never survive `a > 100`.
        write_pipelined_test_parquet(&path, 200, 64, true);
        let uri = path.to_str().unwrap().to_string();

        let pred = resolved_col("a")
            .gt(lit(100))
            .and(resolved_col("b").lt(lit(10)));
        let batch = read_pipelined(&uri, Some(pred), vec!["a".into(), "b".into(), "c".into()]);

        let expected: Vec<(Option<i64>, i64, i64)> = pipelined_test_rows(200, true)
            .into_iter()
            .filter(|(a, b, _)| a.is_some_and(|av| av > 100) && *b < 10)
            .collect();
        assert_eq!(batch.len(), expected.len());
        // No null `a` may appear in the output.
        assert!(
            pipelined_col_i64(&batch, "a").iter().all(|a| a.is_some()),
            "null `a` rows must be excluded by `a > 100`"
        );
        assert_eq!(
            pipelined_col_i64(&batch, "c"),
            expected
                .iter()
                .map(|(_, _, c)| Some(*c))
                .collect::<Vec<_>>()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
