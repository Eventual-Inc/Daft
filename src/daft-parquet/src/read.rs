use std::{collections::BTreeMap, sync::Arc, time::Duration};

use arrow::array::ArrayRef;
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

    /// Construct from raw Python-facing inputs (parses `string_encoding`).
    #[cfg(feature = "python")]
    pub fn from_python(
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
        string_encoding: &str,
    ) -> crate::Result<Self> {
        let string_encoding: StringEncoding =
            string_encoding
                .parse()
                .map_err(|e: common_error::DaftError| crate::Error::ArrowError {
                    source: arrow::error::ArrowError::InvalidArgumentError(e.to_string()),
                })?;
        Ok(Self {
            coerce_int96_timestamp_unit: coerce_int96_timestamp_unit
                .map_or(TimeUnit::Nanoseconds, From::from),
            string_encoding,
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
    pub metadata: Option<Arc<DaftParquetMetadata>>,
}

/// Per-file overrides for [`ParquetBulkReadOptions`].
#[derive(Default, Clone)]
pub struct PerFileOptions {
    pub row_groups: Option<Vec<i64>>,
    pub delete_rows: Option<Vec<i64>>,
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

fn parse_source(uri: &str) -> DaftResult<(SourceType, String)> {
    let (source_type, fixed_uri) = parse_url(uri)?;
    let path = if matches!(source_type, SourceType::File) {
        daft_io::strip_file_uri_to_path(&fixed_uri)
            .unwrap_or(&fixed_uri)
            .to_string()
    } else {
        fixed_uri.to_string()
    };
    Ok((source_type, path))
}

fn build_reader_source<'a>(
    uri: &'a str,
    local_path: &'a str,
    source_type: SourceType,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> crate::reader::ParquetSource<'a> {
    if matches!(source_type, SourceType::File) {
        crate::reader::ParquetSource::Local { path: local_path }
    } else {
        crate::reader::ParquetSource::Url {
            uri,
            io_client,
            io_stats,
        }
    }
}

/// Stream a single parquet file as `RecordBatch`es. Returns the projected
/// schema (so callers can construct a schema-bearing empty batch if the stream
/// is empty).
pub async fn stream_parquet(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetReadOptions,
) -> DaftResult<(Arc<Schema>, BoxStream<'static, DaftResult<RecordBatch>>)> {
    let columns_ref: Option<Vec<&str>> = opts
        .columns
        .as_ref()
        .map(|v| v.iter().map(String::as_str).collect());

    let (source_type, local_path) = parse_source(uri)?;
    let source = build_reader_source(uri, &local_path, source_type, io_client, io_stats);

    let (schema, table_stream) = crate::reader::stream_parquet(
        source,
        columns_ref.as_deref(),
        opts.start_offset,
        opts.num_rows,
        opts.row_groups.as_deref(),
        opts.predicate.clone(),
        opts.schema_infer,
        opts.batch_size,
        opts.field_id_mapping.clone(),
        opts.delete_rows.as_deref(),
    )
    .await?;

    // Cap total rows across the stream.
    let mut remaining_rows = opts.num_rows.map(|limit| limit as i64);
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
    Ok((schema, stream.boxed()))
}

/// Read a single parquet file into one concatenated `RecordBatch`.
pub async fn read_parquet(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetReadOptions,
) -> DaftResult<RecordBatch> {
    let columns_ref: Option<Vec<&str>> = opts
        .columns
        .as_ref()
        .map(|v| v.iter().map(String::as_str).collect());

    let (source_type, local_path) = parse_source(uri)?;
    let source = build_reader_source(uri, &local_path, source_type, io_client, io_stats);

    crate::reader::read_parquet(
        source,
        columns_ref.as_deref(),
        opts.start_offset,
        opts.num_rows,
        opts.row_groups.as_deref(),
        opts.predicate,
        opts.schema_infer,
        opts.batch_size,
        opts.field_id_mapping,
        opts.delete_rows.as_deref(),
    )
    .await
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
    // Predicate not supported on the pyarrow path; drop it before the bulk read.
    let mut data_opts = opts.clone();
    data_opts.predicate = None;
    data_opts.delete_rows = None;
    data_opts.batch_size = None;

    let data_fut = read_parquet(uri, io_client.clone(), io_stats.clone(), data_opts);
    let metadata_fut =
        crate::metadata::fetch_parquet_metadata(uri, None, io_client, io_stats, None, None);
    let (rb, parquet_metadata) =
        futures::future::try_join(data_fut, metadata_fut.err_into()).await?;
    let num_rows_read = rb.len();

    // Recover schema-level kv metadata + per-field nullability from parquet.
    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        parquet_metadata.file_metadata().schema_descr(),
        parquet_metadata.file_metadata().key_value_metadata(),
    )
    .ok();
    let schema_metadata: std::collections::HashMap<String, String> = arrow_schema
        .as_ref()
        .map(|s| {
            s.metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .unwrap_or_default();

    // Column-major chunks: all_arrays[col_idx] = [chunks_for_col].
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

/// Sync wrapper for Python bindings. Optionally enforces a wall-clock timeout.
pub fn read_parquet_into_pyarrow(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    opts: ParquetReadOptions,
    file_timeout_ms: Option<i64>,
) -> DaftResult<ParquetPyarrowChunk> {
    let runtime_handle = get_io_runtime(multithreaded_io);
    runtime_handle.block_on_current_thread(async {
        let fut = read_parquet_into_arrow(uri, io_client, io_stats, opts);
        match file_timeout_ms {
            Some(timeout) => {
                match tokio::time::timeout(Duration::from_millis(timeout as u64), fut).await {
                    Ok(result) => result,
                    Err(_) => Err(crate::Error::FileReadTimeout {
                        path: uri.to_string(),
                        duration_ms: timeout,
                    }
                    .into()),
                }
            }
            None => fut.await,
        }
    })
}

/// Read N parquet files concurrently, returning per-file `RecordBatch`es in order.
pub async fn read_parquet_bulk(
    uris: Vec<String>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    opts: ParquetBulkReadOptions,
) -> DaftResult<Vec<DaftResult<RecordBatch>>> {
    if !opts.per_file.is_empty() && opts.per_file.len() != uris.len() {
        return Err(common_error::DaftError::ValueError(format!(
            "Mismatch of length of `uris` and `per_file`. {} vs {}",
            uris.len(),
            opts.per_file.len()
        )));
    }

    let num_parallel = opts.num_parallel_tasks.max(1);
    let task_stream = futures::stream::iter(uris.into_iter().enumerate().map(|(i, uri)| {
        let per = opts.per_file.get(i).cloned().unwrap_or_default();
        let single_opts = ParquetReadOptions {
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
        };
        let io_client = io_client.clone();
        let io_stats = io_stats.clone();
        tokio::task::spawn(
            async move { read_parquet(&uri, io_client, io_stats, single_opts).await },
        )
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
        .await
        .context(JoinSnafu { path: "UNKNOWN" })?;
    Ok(tables)
}

/// Sync entry for callers off the tokio runtime (Python bindings, micropartition).
pub fn read_parquet_bulk_sync(
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    opts: ParquetBulkReadOptions,
) -> DaftResult<Vec<RecordBatch>> {
    let runtime = get_io_runtime(multithreaded_io);
    let uris_owned: Vec<String> = uris.iter().map(|s| (*s).to_string()).collect();
    runtime
        .block_on_current_thread(read_parquet_bulk(uris_owned, io_client, io_stats, opts))?
        .into_iter()
        .collect()
}

/// Bulk pyarrow read. Returns chunks in input order.
pub fn read_parquet_into_pyarrow_bulk(
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    opts: ParquetBulkReadOptions,
) -> DaftResult<Vec<ParquetPyarrowChunk>> {
    if !opts.per_file.is_empty() && opts.per_file.len() != uris.len() {
        return Err(common_error::DaftError::ValueError(format!(
            "Mismatch of length of `uris` and `per_file`. {} vs {}",
            uris.len(),
            opts.per_file.len()
        )));
    }
    let runtime = get_io_runtime(multithreaded_io);
    let num_parallel = opts.num_parallel_tasks.max(1);
    let results = runtime
        .block_on_current_thread(async move {
            futures::stream::iter(uris.iter().enumerate().map(|(i, uri)| {
                let uri = (*uri).to_string();
                let per = opts.per_file.get(i).cloned().unwrap_or_default();
                let single_opts = ParquetReadOptions {
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
                };
                let io_client = io_client.clone();
                let io_stats = io_stats.clone();
                tokio::task::spawn(async move {
                    Ok((
                        i,
                        read_parquet_into_arrow(&uri, io_client, io_stats, single_opts).await?,
                    ))
                })
            }))
            .buffer_unordered(num_parallel)
            .try_collect::<Vec<_>>()
            .await
        })
        .context(JoinSnafu { path: "UNKNOWN" })?;
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
    let metadata = crate::metadata::fetch_parquet_metadata(
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
    let metadata = crate::metadata::fetch_parquet_metadata(
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
    let handles = uris.iter().map(|uri| {
        let uri = (*uri).to_string();
        let io_client = io_client.clone();
        let io_stats = io_stats.clone();
        let field_id_mapping = field_id_mapping.clone();
        tokio::spawn(async move {
            read_parquet_metadata(&uri, io_client, io_stats, field_id_mapping).await
        })
    });
    try_join_all(handles)
        .await
        .context(JoinSnafu { path: "BULK READ" })?
        .into_iter()
        .collect()
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

    let runtime = get_io_runtime(true);
    let handles = path_array.into_iter().map(|uri| {
        let uri = uri.map(std::string::ToString::to_string);
        let io_client = io_client.clone();
        let io_stats = io_stats.clone();
        let field_id_mapping = field_id_mapping.clone();
        tokio::spawn(async move {
            match uri {
                Some(uri) => {
                    let m =
                        read_parquet_metadata(&uri, io_client, io_stats, field_id_mapping).await?;
                    Ok((
                        Some(m.num_rows()),
                        Some(m.num_row_groups()),
                        Some(m.version()),
                    ))
                }
                None => Ok((None, None, None)),
            }
        })
    });
    let tuples = runtime.block_on_current_thread(async { join_all(handles).await });
    let all = tuples
        .into_iter()
        .zip(path_array.into_iter())
        .map(|(t, u)| {
            t.with_context(|_| JoinSnafu {
                path: u.unwrap().to_string(),
            })?
        })
        .collect::<DaftResult<Vec<_>>>()?;
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
        d.push("../../");
        d.push(PARQUET_FILE_LOCAL);
        d.to_str().unwrap().to_string()
    }

    #[test]
    fn test_parquet_read_from_s3() -> DaftResult<()> {
        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let runtime = get_io_runtime(true);
        let table = runtime.block_on_current_thread(read_parquet(
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
            let (_schema, stream) =
                stream_parquet(PARQUET_FILE, io_client, None, ParquetReadOptions::default())
                    .await?;
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

        let total_rows: usize = runtime
            .block_within_async_context(async move {
                let opts = ParquetReadOptions {
                    num_rows: Some(5),
                    ..Default::default()
                };
                let (_schema, mut stream) =
                    stream_parquet(&uri, io_client, None, opts).await.unwrap();
                let mut count = 0;
                while let Some(batch) = stream.next().await {
                    count += batch.unwrap().len();
                }
                count
            })
            .unwrap();

        assert_eq!(total_rows, 5);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
