use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Display,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::ArrayRef;
use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_core::prelude::*;
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_dsl::{AggExpr, Expr, ExprRef};
use daft_io::{IOClient, IOConfig, IOStatsRef};
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_parquet::{
    DaftParquetMetadata,
    read::{
        ParquetBulkReadOptions, ParquetSchemaInferenceOptions, PerFileOptions,
        read_parquet_bulk_sync, read_parquet_metadata_bulk,
    },
};
use daft_recordbatch::RecordBatch;
use daft_stats::{ColumnRangeStatistics, PartitionSpec, TableMetadata, TableStatistics};
use daft_warc::WarcConvertOptions;
use futures::{Future, Stream, TryStreamExt};
use snafu::ResultExt;

use crate::DaftCoreComputeSnafu;

pub type MicroPartitionRef = Arc<MicroPartition>;

#[derive(Debug, Clone)]
pub struct MicroPartition {
    /// Schema of the MicroPartition
    ///
    /// This is technically redundant with the schema of the record batches in `chunks`.
    /// However this is still useful as an easy-to-access copy of the schema, as well as to handle the corner-case
    /// of having 0 record batches (in an empty [`MicroPartition`])
    pub(crate) schema: SchemaRef,

    pub(crate) chunks: Arc<Vec<RecordBatch>>,

    /// Metadata about the MicroPartition
    pub(crate) metadata: TableMetadata,

    /// Statistics about the MicroPartition
    ///
    /// If present, this must have the same [`Schema`] as [`MicroPartition::schema`], and this invariant
    /// is enforced in the `MicroPartition::new_*` constructors.
    pub(crate) statistics: Option<TableStatistics>,
}

impl MicroPartition {
    /// Create a new "loaded" MicroPartition using the materialized record batches.
    ///
    /// Schema invariants:
    /// 1. `schema` must match each record batch's schema exactly
    /// 2. If `statistics` is provided, each Loaded column statistic must be castable to the corresponding column in the MicroPartition's schema
    #[must_use]
    pub fn new_loaded(
        schema: SchemaRef,
        record_batches: Arc<Vec<RecordBatch>>,
        statistics: Option<TableStatistics>,
    ) -> Self {
        // Check and validate invariants with asserts
        for batch in record_batches.iter() {
            assert!(
                batch.schema == schema,
                "Loaded MicroPartition's batch schema must match its own schema exactly, found {} vs {}",
                batch.schema,
                schema,
            );
        }

        if let Some(stats) = &statistics {
            assert_eq!(
                stats.schema(),
                schema.as_ref(),
                "Loaded MicroPartition's statistics schema must match its own schema exactly, found {} vs {}",
                stats.schema(),
                schema.as_ref(),
            );
        }

        // micropartition length is the length of all batches combined
        let length = record_batches
            .iter()
            .map(daft_recordbatch::RecordBatch::len)
            .sum();

        Self {
            schema,
            chunks: record_batches,
            metadata: TableMetadata {
                length,
                column_sizes: None,
            },
            statistics,
        }
    }

    pub fn from_arrow<S: Into<SchemaRef>>(schema: S, arrays: Vec<ArrayRef>) -> DaftResult<Self> {
        let schema = schema.into();
        let batch = RecordBatch::from_arrow(schema.clone(), arrays)?;
        let batches = Arc::new(vec![batch]);
        Ok(Self::new_loaded(schema, batches, None))
    }

    #[must_use]
    pub fn empty(schema: Option<SchemaRef>) -> Self {
        let schema = schema.unwrap_or_else(|| Schema::empty().into());
        Self::new_loaded(schema, Arc::new(vec![]), None)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.schema.names()
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn len(&self) -> usize {
        self.metadata.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn record_batches(&self) -> &[RecordBatch] {
        self.chunks.as_slice()
    }

    pub fn size_bytes(&self) -> usize {
        self.record_batches()
            .iter()
            .map(daft_recordbatch::RecordBatch::size_bytes)
            .sum()
    }

    pub fn concat_or_get(&self) -> crate::Result<Option<RecordBatch>> {
        match self.record_batches() {
            [] => Ok(None),
            // Avoid the clone here?
            [table] => Ok(Some(table.clone())),
            tables => {
                let new_table = RecordBatch::concat(tables.iter().collect::<Vec<_>>().as_slice())
                    .context(DaftCoreComputeSnafu)?;
                // Have a variant fn that updates the existing MicroPartition with new_table
                // like concat_or_get_and_update?
                Ok(Some(new_table))
            }
        }
    }

    pub fn add_monotonically_increasing_id(
        &self,
        partition_num: u64,
        column_name: &str,
    ) -> DaftResult<Self> {
        let tables = self.record_batches();

        let tables_with_id = tables
            .iter()
            .scan(0u64, |offset, table| {
                let table_with_id =
                    table.add_monotonically_increasing_id(partition_num, *offset, column_name);
                *offset += table.len() as u64;
                Some(table_with_id)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let schema_with_id_index_map = std::iter::once(Field::new(column_name, DataType::UInt64))
            .chain(self.schema.into_iter().cloned());

        let schema_with_id = Arc::new(Schema::new(schema_with_id_index_map));

        let stats = self.statistics.as_ref().map(|stats| {
            let columns = std::iter::once(&ColumnRangeStatistics::Missing)
                .chain(stats)
                .cloned()
                .collect();

            TableStatistics::new(columns, schema_with_id.clone())
        });

        Ok(Self::new_loaded(
            schema_with_id,
            Arc::new(tables_with_id),
            stats,
        ))
    }

    pub fn write_to_ipc_stream(&self) -> DaftResult<Vec<u8>> {
        let mut buffer = Vec::with_capacity(self.size_bytes());
        let arrow_schema = self.schema.to_arrow()?;
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buffer, &arrow_schema)?;

        for table in self.record_batches() {
            // Convert daft RecordBatch to arrow-rs RecordBatch
            let arrow_batch: arrow_array::RecordBatch = table.clone().try_into()?;
            writer.write(&arrow_batch)?;
        }

        writer.finish()?;
        buffer.shrink_to_fit();
        Ok(buffer)
    }

    pub fn read_from_ipc_stream(buffer: &[u8]) -> DaftResult<Self> {
        let mut cursor = std::io::Cursor::new(buffer);
        let reader = arrow_ipc::reader::StreamReader::try_new(&mut cursor, None)?;

        let arrow_schema = reader.schema();
        let schema: SchemaRef = Arc::new(arrow_schema.as_ref().try_into()?);

        // Read all record batches
        let mut tables = Vec::new();
        for arrow_batch_result in reader {
            let arrow_batch = arrow_batch_result?;
            let arrow_arrays: Vec<ArrayRef> = arrow_batch.columns().to_vec();

            let record_batch = RecordBatch::from_arrow(schema.clone(), arrow_arrays)?;
            tables.push(record_batch);
        }

        Ok(Self::new_loaded(schema, Arc::new(tables), None))
    }

    /// Decode a buffer holding one or more **concatenated** Arrow IPC streams
    /// into a flat list of `RecordBatch`es.
    ///
    /// Celeborn stores one self-contained IPC stream per map-side push, so a
    /// reduce partition read back is a concatenation of them and each needs its
    /// own decoder. Zero-row batches are dropped, and the Arrow schema is
    /// converted to a Daft `Schema` once and shared by every batch instead of
    /// once per push.
    ///
    /// Takes the transport bytes by value so the decoded arrays can borrow from
    /// them instead of being copied out. Misaligned array data still falls back
    /// to a copy inside the decoder. The caller must hold no other reference to
    /// the bytes, or the arrays keep the whole buffer alive.
    pub fn read_record_batches_from_ipc_streams(
        buffer: bytes::Bytes,
    ) -> DaftResult<Vec<RecordBatch>> {
        // Shares the `bytes::Bytes` allocation rather than copying it: the
        // conversion just moves the pointer into an `Arc` deallocation handle.
        let mut buffer = arrow::buffer::Buffer::from(buffer);
        let mut out = Vec::new();
        // Keyed by the *Arrow* schema so the equality check never has to redo
        // the (allocating) Arrow -> Daft conversion just to compare.
        let mut converted: Option<(arrow::datatypes::SchemaRef, SchemaRef)> = None;
        let mut decoder = arrow_ipc::reader::StreamDecoder::new();

        while !buffer.is_empty() {
            let arrow_batch = match decoder.decode(&mut buffer) {
                Ok(Some(arrow_batch)) => arrow_batch,
                // Buffer exhausted without completing a message. Either a clean
                // end (the `finish` below accepts it) or truncated data.
                Ok(None) => break,
                Err(e) => {
                    // A decoder latches to its finished state at the stream's
                    // end-of-stream marker and reports whatever follows as an
                    // error. Here that is the normal case, not a failure: the
                    // next map-side push starts, so give it a fresh decoder.
                    // `finish` distinguishes the two — a decoder that stopped on
                    // a real error is left mid-message and rejects it.
                    if decoder.finish().is_err() {
                        return Err(e.into());
                    }
                    decoder = arrow_ipc::reader::StreamDecoder::new();
                    continue;
                }
            };

            let (arrow_schema, arrow_arrays, num_rows) = arrow_batch.into_parts();
            // Drop empty batches here so callers never have to: they carry no
            // rows, so all they would do is pad every downstream concat and the
            // size accounting that decides Morsel boundaries. A partition made
            // only of them therefore decodes to nothing, which leaves the reduce
            // reader on its `sent_any == false` path — the one that emits the
            // single empty Morsel the pipeline contract wants, with the declared
            // schema (see `shuffle_read.rs`).
            if num_rows == 0 {
                continue;
            }
            let schema = match &converted {
                // Every push writes the same schema, so reuse the converted one.
                // Batches within one stream even share the schema `Arc`, which
                // the pointer check settles without walking the field list.
                Some((prev, daft)) if Arc::ptr_eq(prev, &arrow_schema) || prev == &arrow_schema => {
                    Arc::clone(daft)
                }
                // Arrow schema equality covers metadata, which a Daft schema does
                // not carry, so two pushes that are identical as far as the engine
                // is concerned can still land here. Re-check on the Daft side and
                // only reject what a concat of the decoded batches would reject
                // anyway.
                Some((_, daft)) => {
                    let daft = Arc::clone(daft);
                    let next: SchemaRef = Arc::new(arrow_schema.as_ref().try_into()?);
                    if next != daft {
                        return Err(DaftError::SchemaMismatch(format!(
                            "concatenated IPC streams have mismatched schemas: {daft} vs {next}"
                        )));
                    }
                    // Re-key the cache on this stream's Arrow schema so its
                    // remaining batches take the cheap path again, and keep handing
                    // out the one Daft schema `Arc` — `MicroPartition::new_loaded`
                    // compares every batch's schema, which a shared `Arc` settles
                    // by pointer.
                    converted = Some((arrow_schema, Arc::clone(&daft)));
                    daft
                }
                None => {
                    let daft: SchemaRef = Arc::new(arrow_schema.as_ref().try_into()?);
                    converted = Some((arrow_schema, daft.clone()));
                    daft
                }
            };

            out.push(RecordBatch::from_arrow(schema, arrow_arrays)?);
        }

        // Catches a partition truncated mid-message, which would otherwise be
        // silently short a batch.
        decoder.finish()?;
        Ok(out)
    }
}

fn prune_fields_from_schema(
    schema: Arc<Schema>,
    columns: Option<&[&str]>,
) -> DaftResult<Arc<Schema>> {
    if let Some(columns) = columns {
        let avail_names = schema.field_names().collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name) {
                names_to_keep.insert(*col_name);
            } else {
                return Err(super::Error::FieldNotFound {
                    field: (*col_name).to_string(),
                    available_fields: avail_names.iter().map(|v| (*v).to_string()).collect(),
                }
                .into());
            }
        }
        let filtered_columns = schema
            .into_iter()
            .filter(|field| names_to_keep.contains(field.name.as_ref()))
            .cloned();
        Ok(Arc::new(Schema::new(filtered_columns)))
    } else {
        Ok(schema)
    }
}

pub fn read_csv_into_micropartition(
    uris: &[&str],
    convert_options: Option<CsvConvertOptions>,
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    io_config: Arc<IOConfig>,
    multithreaded_io: bool,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<MicroPartition> {
    let io_client = daft_io::get_io_client(multithreaded_io, io_config)?;

    match uris {
        [] => Ok(MicroPartition::empty(None)),
        uris => {
            // Perform a bulk read of URIs, materializing a table per URI.
            let tables = daft_csv::read_csv_bulk(
                uris,
                convert_options,
                parse_options,
                read_options,
                io_client,
                io_stats,
                multithreaded_io,
                None,
                8,
            )
            .context(DaftCoreComputeSnafu)?;

            // Union all schemas and cast all tables to the same schema
            let unioned_schema = tables
                .iter()
                .map(|tbl| tbl.schema.clone())
                .try_reduce(|s1, s2| s1.non_distinct_union(s2.as_ref()).map(Arc::new))?
                .unwrap();
            let tables = tables
                .into_iter()
                .map(|tbl| {
                    #[allow(deprecated)]
                    tbl.cast_to_schema(&unioned_schema)
                })
                .collect::<DaftResult<Vec<_>>>()?;

            // Construct MicroPartition from tables and unioned schema
            Ok(MicroPartition::new_loaded(
                unioned_schema,
                Arc::new(tables),
                None,
            ))
        }
    }
}

/// Reads JSON/JSONL files into a MicroPartition.
///
/// Exposed to Python via `MicroPartition.read_json()`.
/// Only used by test paths (`tests/recordbatch/` and `tests/integration/io/jsonl/`).
/// The production path uses `stream_scan_task` → `stream_json` instead.
pub fn read_json_into_micropartition(
    uris: &[&str],
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
    io_config: Arc<IOConfig>,
    multithreaded_io: bool,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<MicroPartition> {
    let io_client = daft_io::get_io_client(multithreaded_io, io_config)?;

    match uris {
        [] => Ok(MicroPartition::empty(None)),
        uris => {
            let schema_hint = convert_options
                .as_ref()
                .and_then(|opts| opts.schema.clone());
            let runtime_handle = get_io_runtime(multithreaded_io);
            let mut remaining_rows = convert_options
                .as_ref()
                .and_then(|opts| opts.limit.map(|limit| limit as i64));
            let tables = runtime_handle
                .block_on_current_thread(async move {
                    let mut out = Vec::new();
                    for uri in uris {
                        if remaining_rows.is_some_and(|rows_left| rows_left <= 0) {
                            break;
                        }

                        let convert_options_for_uri = convert_options.clone().map(|mut co| {
                            if let Some(rows_left) = remaining_rows {
                                co.limit = Some(usize::try_from(rows_left.max(0)).unwrap_or(0));
                            }
                            co
                        });

                        let stream = daft_json::read::stream_json(
                            (*uri).to_string(),
                            convert_options_for_uri,
                            parse_options.clone(),
                            read_options.clone(),
                            io_client.clone(),
                            io_stats.clone(),
                            None,
                            None,
                        )
                        .await?;

                        let mut tables = stream.try_collect::<Vec<_>>().await?;
                        if let Some(rows_left) = remaining_rows {
                            let read_rows = tables.iter().map(|t| t.len() as i64).sum::<i64>();
                            remaining_rows = Some(rows_left - read_rows);
                        }
                        out.append(&mut tables);
                    }
                    DaftResult::Ok(out)
                })
                .context(DaftCoreComputeSnafu)?;

            if tables.is_empty() {
                return Ok(MicroPartition::empty(schema_hint));
            }

            // Union all schemas and cast all tables to the same schema
            let unioned_schema = tables
                .iter()
                .map(|tbl| tbl.schema.clone())
                .try_reduce(|s1, s2| s1.non_distinct_union(s2.as_ref()).map(Arc::new))?
                .unwrap();
            let tables = tables
                .into_iter()
                .map(|tbl| {
                    #[allow(deprecated)]
                    tbl.cast_to_schema(&unioned_schema)
                })
                .collect::<DaftResult<Vec<_>>>()?;

            // Construct MicroPartition from tables and unioned schema
            Ok(MicroPartition::new_loaded(
                unioned_schema,
                Arc::new(tables),
                None,
            ))
        }
    }
}

pub fn read_warc_into_micropartition(
    uris: &[&str],
    schema: SchemaRef,
    io_config: Arc<IOConfig>,
    multithreaded_io: bool,
    io_stats: IOStatsRef,
) -> DaftResult<MicroPartition> {
    let io_client = daft_io::get_io_client(multithreaded_io, io_config)?;
    let convert_options = WarcConvertOptions {
        limit: None,
        include_columns: None,
        schema: schema.clone(),
        predicate: None,
    };

    match uris {
        [] => Ok(MicroPartition::empty(None)),
        uris => {
            // Perform a bulk read of URIs, materializing a table per URI.
            let tables = daft_warc::read_warc_bulk(
                uris,
                convert_options,
                io_client,
                io_stats,
                multithreaded_io,
                None,
                8,
            )
            .context(DaftCoreComputeSnafu)?;

            // Construct MicroPartition from tables and unioned schema
            Ok(MicroPartition::new_loaded(schema, Arc::new(tables), None))
        }
    }
}
fn get_file_column_names<'a>(
    columns: Option<&'a [&'a str]>,
    partition_spec: Option<&PartitionSpec>,
) -> Option<Vec<&'a str>> {
    match (columns, partition_spec.map(|ps| ps.to_fill_map())) {
        (None, _) => None,
        (Some(columns), None) => Some(columns.to_vec()),

        // If the ScanTask has a partition_spec, we elide reads of partition columns from the file
        (Some(columns), Some(partition_fillmap)) => Some(
            columns
                .as_ref()
                .iter()
                .filter_map(|s| {
                    if partition_fillmap.contains_key(s) {
                        None
                    } else {
                        Some(*s)
                    }
                })
                .collect::<Vec<&str>>(),
        ),
    }
}

fn read_delete_files(
    delete_files: &[&str],
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    multithreaded_io: bool,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<HashMap<String, Vec<i64>>> {
    let opts = ParquetBulkReadOptions {
        columns: Some(vec!["file_path".to_string(), "pos".to_string()]),
        schema_infer: *schema_infer_options,
        num_parallel_tasks,
        ..Default::default()
    };
    let tables = read_parquet_bulk_sync(delete_files, io_client, io_stats, multithreaded_io, opts)?;

    let mut delete_map: HashMap<String, Vec<i64>> = uris
        .iter()
        .map(|uri| ((*uri).to_string(), vec![]))
        .collect();

    for table in &tables {
        // values in the file_path column are guaranteed by the iceberg spec to match the full URI of the corresponding data file
        // https://iceberg.apache.org/spec/#position-delete-files

        let get_column_by_name = |name| {
            if let [(idx, _)] = table.schema.get_fields_with_name(name)[..] {
                Ok(table.get_column(idx))
            } else {
                Err(DaftError::SchemaMismatch(format!(
                    "Iceberg delete files must have columns \"file_path\" and \"pos\", found: {}",
                    table.schema
                )))
            }
        };

        let file_paths = get_column_by_name("file_path")?.downcast::<Utf8Array>()?;
        let positions = get_column_by_name("pos")?.downcast::<Int64Array>()?;

        for i in 0..table.len() {
            let file = file_paths.get(i);
            let pos = positions.get(i);

            if let Some(file) = file
                && let Some(pos) = pos
                && delete_map.contains_key(file)
            {
                delete_map.get_mut(file).unwrap().push(pos);
            }
        }
    }

    Ok(delete_map)
}

#[allow(clippy::too_many_arguments)]
fn read_parquet_into_loaded_micropartition<T: AsRef<str>>(
    io_client: Arc<IOClient>,
    multithreaded_io: bool,
    uris: &[&str],
    columns: Option<&[T]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    iceberg_delete_files: Option<Vec<&str>>,
    row_groups: Option<Vec<Option<Vec<i64>>>>,
    predicate: Option<ExprRef>,
    partition_spec: Option<&PartitionSpec>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    schema_infer_options: &ParquetSchemaInferenceOptions,
    catalog_provided_schema: Option<SchemaRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    chunk_size: Option<usize>,
) -> DaftResult<MicroPartition> {
    let delete_map = iceberg_delete_files
        .map(|files| {
            read_delete_files(
                files.as_slice(),
                uris,
                io_client.clone(),
                io_stats.clone(),
                num_parallel_tasks,
                multithreaded_io,
                schema_infer_options,
            )
        })
        .transpose()?;

    let columns = columns.map(|cols| {
        cols.iter()
            .map(std::convert::AsRef::as_ref)
            .collect::<Vec<&str>>()
    });

    let file_column_names = get_file_column_names(columns.as_deref(), partition_spec);
    let row_groups = row_groups.as_deref();
    let per_file: Vec<PerFileOptions> = if row_groups.is_none() && delete_map.is_none() {
        Vec::new()
    } else {
        uris.iter()
            .enumerate()
            .map(|(i, uri)| PerFileOptions {
                row_groups: row_groups.and_then(|rgs| rgs[i].clone()),
                delete_rows: delete_map.as_ref().and_then(|m| m.get(*uri).cloned()),
                metadata: None,
            })
            .collect()
    };
    let opts = ParquetBulkReadOptions {
        columns: file_column_names.map(|v| v.into_iter().map(str::to_string).collect()),
        start_offset,
        num_rows,
        predicate,
        schema_infer: *schema_infer_options,
        field_id_mapping,
        batch_size: chunk_size,
        num_parallel_tasks,
        per_file,
    };
    let all_tables = read_parquet_bulk_sync(uris, io_client, io_stats, multithreaded_io, opts)?;

    // Prefer using the `catalog_provided_schema` but fall back onto inferred schema from Parquet files
    let full_daft_schema = if let Some(catalog_provided_schema) = catalog_provided_schema {
        catalog_provided_schema
    } else {
        let unioned_schema = all_tables
            .iter()
            .map(|t| t.schema.clone())
            .try_reduce(|l, r| l.non_distinct_union(&r).map(Arc::new))?;
        unioned_schema.expect("we need at least 1 schema")
    };

    let pruned_daft_schema = prune_fields_from_schema(full_daft_schema, columns.as_deref())?;

    let fill_map = partition_spec.map(|pspec| pspec.to_fill_map());
    let all_tables = all_tables
        .into_iter()
        .map(|t| {
            #[allow(deprecated)]
            t.cast_to_schema_with_fill(&pruned_daft_schema, fill_map.as_ref())
        })
        .collect::<DaftResult<Vec<_>>>()?;

    // TODO: we can pass in stats here to optimize downstream workloads such as join. Make sure to correctly
    // cast those statistics to the appropriate schema + fillmap as well.
    Ok(MicroPartition::new_loaded(
        pruned_daft_schema,
        all_tables.into(),
        None,
    ))
}

#[allow(clippy::too_many_arguments)]
pub fn read_parquet_into_micropartition<T: AsRef<str>>(
    uris: &[&str],
    columns: Option<&[T]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    iceberg_delete_files: Option<Vec<&str>>,
    row_groups: Option<Vec<Option<Vec<i64>>>>,
    predicate: Option<ExprRef>,
    partition_spec: Option<&PartitionSpec>,
    io_config: Arc<IOConfig>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    multithreaded_io: bool,
    schema_infer_options: &ParquetSchemaInferenceOptions,
    catalog_provided_schema: Option<SchemaRef>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    parquet_metadata: Option<Vec<Arc<DaftParquetMetadata>>>,
    chunk_size: Option<usize>,
    aggregation_pushdown: Option<&Expr>,
) -> DaftResult<MicroPartition> {
    if let Some(so) = start_offset
        && so > 0
    {
        return Err(common_error::DaftError::ValueError(
            "Micropartition Parquet Reader does not support non-zero start offsets".to_string(),
        ));
    }

    // Run the required I/O to retrieve all the Parquet FileMetaData
    let io_client = daft_io::get_io_client(multithreaded_io, io_config)?;

    // If we have a predicate or deletion files then we no longer have an accurate accounting of required metadata
    // on the MicroPartition (e.g. its length). Hence we need to perform an eager read.
    if iceberg_delete_files
        .as_ref()
        .is_some_and(|files| !files.is_empty())
        || predicate.is_some()
    {
        return read_parquet_into_loaded_micropartition(
            io_client,
            multithreaded_io,
            uris,
            columns,
            start_offset,
            num_rows,
            iceberg_delete_files,
            row_groups,
            predicate,
            partition_spec,
            io_stats,
            num_parallel_tasks,
            schema_infer_options,
            catalog_provided_schema,
            field_id_mapping,
            chunk_size,
        );
    }
    let runtime_handle = get_io_runtime(multithreaded_io);
    // Attempt to read TableStatistics from the Parquet file
    let meta_io_client = io_client.clone();
    let meta_io_stats = io_stats.clone();
    let meta_field_id_mapping = field_id_mapping.clone();
    let (metadata, _) = if let Some(metadata) = parquet_metadata {
        let schemas = metadata
            .iter()
            .map(|m| {
                let daft_schema =
                    daft_parquet::infer_schema_from_daft_metadata(m, *schema_infer_options)?;
                DaftResult::Ok(Arc::new(daft_schema))
            })
            .collect::<DaftResult<Vec<_>>>()?;
        (metadata, schemas)
    } else {
        let metadata = runtime_handle
            .block_on_current_thread(async move {
                read_parquet_metadata_bulk(
                    uris,
                    meta_io_client,
                    meta_io_stats,
                    meta_field_id_mapping,
                )
                .await
            })?
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();

        let schemas = metadata
            .iter()
            .map(|m| {
                let daft_schema =
                    daft_parquet::infer_schema_from_daft_metadata(m, *schema_infer_options)?;
                DaftResult::Ok(Arc::new(daft_schema))
            })
            .collect::<DaftResult<Vec<_>>>()?;
        (metadata, schemas)
    };

    // Handle count pushdown aggregation optimization.
    if let Some(Expr::Agg(AggExpr::Count(_, _))) = aggregation_pushdown {
        let count: usize = metadata.iter().map(|m| m.num_rows()).sum();
        let count_field = daft_core::datatypes::Field::new(
            aggregation_pushdown.unwrap().name(),
            daft_core::datatypes::DataType::UInt64,
        );
        let count_array =
            UInt64Array::from_iter(count_field.clone(), std::iter::once(Some(count as u64)));
        let count_batch = daft_recordbatch::RecordBatch::new_with_size(
            Schema::new(vec![count_field]),
            vec![count_array.into_series()],
            1,
        )
        .context(DaftCoreComputeSnafu)?;
        return Ok(MicroPartition::new_loaded(
            count_batch.schema.clone(),
            Arc::new(vec![count_batch]),
            None,
        ));
    }

    // If no TableStatistics are available, we perform an eager read
    read_parquet_into_loaded_micropartition(
        io_client,
        multithreaded_io,
        uris,
        columns,
        start_offset,
        num_rows,
        iceberg_delete_files,
        row_groups,
        predicate,
        partition_spec,
        io_stats,
        num_parallel_tasks,
        schema_infer_options,
        catalog_provided_schema,
        field_id_mapping,
        chunk_size,
    )
}

impl Display for MicroPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MicroPartition with {} rows:", self.len())?;

        if self.chunks.is_empty() {
            writeln!(f, "{}", self.schema)?;
        }
        for chunk in self.chunks.iter() {
            writeln!(f, "{chunk}")?;
        }

        match &self.statistics {
            Some(t) => writeln!(f, "Statistics\n{t}")?,
            None => writeln!(f, "Statistics: missing")?,
        }

        Ok(())
    }
}

struct MicroPartitionStreamAdapter {
    state: Arc<Vec<RecordBatch>>,
    current: usize,
    pending_task: Option<tokio::task::JoinHandle<DaftResult<Vec<RecordBatch>>>>,
}

impl Stream for MicroPartitionStreamAdapter {
    type Item = DaftResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(handle) = &mut this.pending_task {
            match Pin::new(handle).poll(cx) {
                Poll::Ready(Ok(Ok(tables))) => {
                    let tables = Arc::new(tables);
                    this.state = tables.clone();
                    this.current = 0;
                    this.pending_task = None;
                    return Poll::Ready(tables.first().cloned().map(Ok));
                }
                Poll::Ready(Ok(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(DaftError::InternalError(e.to_string()))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        let current = this.current;
        if current < this.state.len() {
            this.current = current + 1;
            Poll::Ready(this.state.get(current).cloned().map(Ok))
        } else {
            Poll::Ready(None)
        }
    }
}

impl MicroPartition {
    pub fn into_stream(self: Arc<Self>) -> DaftResult<impl Stream<Item = DaftResult<RecordBatch>>> {
        Ok(MicroPartitionStreamAdapter {
            state: self.chunks.clone(),
            current: 0,
            pending_task: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::{
        datatypes::{DataType, Field, Float64Array, Int32Array, Utf8Array},
        prelude::Schema,
        series::IntoSeries,
    };
    use daft_recordbatch::RecordBatch;
    use futures::StreamExt;

    use crate::MicroPartition;

    #[tokio::test]
    async fn test_mp_stream() -> DaftResult<()> {
        let columns = vec![Int32Array::from_slice("a", &[1]).into_series()];
        let columns2 = vec![Int32Array::from_slice("a", &[2]).into_series()];
        let schema = Schema::new(vec![Field::new("a", DataType::Int32)]);

        let table1 = RecordBatch::from_nonempty_columns(columns)?;
        let table2 = RecordBatch::from_nonempty_columns(columns2)?;

        let mp = MicroPartition::new_loaded(
            Arc::new(schema),
            Arc::new(vec![table1.clone(), table2.clone()]),
            None,
        );
        let mp = Arc::new(mp);

        let mut stream = mp.into_stream()?;
        let tbl = stream.next().await.unwrap().unwrap();
        assert_eq!(tbl, table1);
        let tbl = stream.next().await.unwrap().unwrap();
        assert_eq!(tbl, table2);
        Ok(())
    }

    #[test]
    fn test_ipc_roundtrip() -> DaftResult<()> {
        let string_values = vec!["a", "bb", "ccc"];
        let batch1 = RecordBatch::from_nonempty_columns(vec![
            Int32Array::from_slice("a", &[1, 2, 3]).into_series(),
            Float64Array::from_slice("b", &[1., 2., 3.]).into_series(),
            Utf8Array::from_slice("c", string_values.as_slice()).into_series(),
        ])?;

        let batch2 = RecordBatch::from_nonempty_columns(vec![
            Int32Array::from_slice("a", &[4, 5, 6]).into_series(),
            Float64Array::from_slice("b", &[4., 5., 6.]).into_series(),
            Utf8Array::from_slice("c", string_values.as_slice()).into_series(),
        ])?;

        assert_eq!(batch1.schema, batch2.schema);

        let table = MicroPartition::new_loaded(
            batch1.schema.clone(),
            Arc::new(vec![batch1.clone(), batch2.clone()]),
            None,
        );

        let ipc_stream = table.write_to_ipc_stream()?;
        let roundtrip_table = MicroPartition::read_from_ipc_stream(&ipc_stream)?;
        assert_eq!(batch1, roundtrip_table.record_batches()[0]);
        assert_eq!(batch2, roundtrip_table.record_batches()[1]);
        Ok(())
    }

    fn int_batch(values: &[i32]) -> DaftResult<RecordBatch> {
        let strings = vec!["x"; values.len()];
        RecordBatch::from_nonempty_columns(vec![
            Int32Array::from_slice("a", values).into_series(),
            Utf8Array::from_slice("b", strings.as_slice()).into_series(),
        ])
    }

    fn ipc_stream_of(batches: Vec<RecordBatch>, schema: &Arc<Schema>) -> DaftResult<Vec<u8>> {
        MicroPartition::new_loaded(schema.clone(), Arc::new(batches), None).write_to_ipc_stream()
    }

    /// A Celeborn reduce partition is the map-side pushes concatenated, so the
    /// buffer holds several self-contained IPC streams back to back — including
    /// pushes that carried no batches at all.
    #[test]
    fn test_ipc_concatenated_streams() -> DaftResult<()> {
        let first = int_batch(&[1, 2])?;
        let second = int_batch(&[3])?;
        let third = int_batch(&[4, 5, 6])?;
        let schema = first.schema.clone();

        let mut buffer = ipc_stream_of(vec![first.clone(), second.clone()], &schema)?;
        buffer.extend_from_slice(&ipc_stream_of(vec![], &schema)?);
        buffer.extend_from_slice(&ipc_stream_of(vec![third.clone()], &schema)?);

        let decoded =
            MicroPartition::read_record_batches_from_ipc_streams(bytes::Bytes::from(buffer))?;
        assert_eq!(decoded, vec![first, second, third]);
        Ok(())
    }

    /// A partition cut short mid-message must fail rather than come back quietly
    /// missing rows.
    #[test]
    fn test_ipc_concatenated_streams_truncated() -> DaftResult<()> {
        let batch = int_batch(&[1, 2, 3])?;
        let schema = batch.schema.clone();
        let mut buffer = ipc_stream_of(vec![batch.clone()], &schema)?;
        buffer.extend_from_slice(&ipc_stream_of(vec![batch], &schema)?);
        buffer.truncate(buffer.len() - 16);

        assert!(
            MicroPartition::read_record_batches_from_ipc_streams(bytes::Bytes::from(buffer))
                .is_err()
        );
        Ok(())
    }

    /// Every push writes the same schema, so a mismatch means something upstream
    /// is wrong; say so here instead of failing obscurely in the concat.
    #[test]
    fn test_ipc_concatenated_streams_schema_mismatch() -> DaftResult<()> {
        let batch = int_batch(&[1, 2])?;
        let other = RecordBatch::from_nonempty_columns(vec![
            Float64Array::from_slice("a", &[1., 2.]).into_series(),
            Utf8Array::from_slice("b", &["x", "x"]).into_series(),
        ])?;

        let mut buffer = ipc_stream_of(vec![batch.clone()], &batch.schema)?;
        buffer.extend_from_slice(&ipc_stream_of(vec![other.clone()], &other.schema)?);

        let err = MicroPartition::read_record_batches_from_ipc_streams(bytes::Bytes::from(buffer))
            .unwrap_err();
        assert!(
            matches!(err, common_error::DaftError::SchemaMismatch(_)),
            "expected a schema mismatch, got {err:?}"
        );
        Ok(())
    }

    /// An Arrow schema carries metadata that a Daft schema does not, so two
    /// pushes the engine considers identical can still differ on the Arrow side.
    /// The read must go through, and all batches must keep sharing one Daft
    /// schema (`MicroPartition::new_loaded` compares every batch's schema).
    #[test]
    fn test_ipc_concatenated_streams_arrow_metadata_differs() -> DaftResult<()> {
        fn ipc_stream_with_metadata(batch: &RecordBatch) -> DaftResult<Vec<u8>> {
            let arrow_batch: arrow_array::RecordBatch = batch.clone().try_into()?;
            let schema = Arc::new(arrow_batch.schema().as_ref().clone().with_metadata(
                std::iter::once(("celeborn.push".to_string(), "7".to_string())).collect(),
            ));
            let arrow_batch =
                arrow_array::RecordBatch::try_new(schema.clone(), arrow_batch.columns().to_vec())?;

            let mut buffer = Vec::new();
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buffer, &schema)?;
            writer.write(&arrow_batch)?;
            writer.finish()?;
            Ok(buffer)
        }

        let first = int_batch(&[1, 2])?;
        let second = int_batch(&[3])?;
        let mut buffer = ipc_stream_of(vec![first.clone()], &first.schema)?;
        buffer.extend_from_slice(&ipc_stream_with_metadata(&second)?);

        let decoded =
            MicroPartition::read_record_batches_from_ipc_streams(bytes::Bytes::from(buffer))?;
        assert_eq!(decoded, vec![first, second]);
        assert!(Arc::ptr_eq(&decoded[0].schema, &decoded[1].schema));
        Ok(())
    }
}
