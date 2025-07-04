use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Display,
    io::Cursor,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use arrow2::io::parquet::read::schema::infer_schema_with_options;
use common_error::{DaftError, DaftResult};
#[cfg(feature = "python")]
use common_file_formats::DatabaseSourceConfig;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_runtime::get_io_runtime;
use common_scan_info::Pushdowns;
use daft_core::prelude::*;
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_dsl::ExprRef;
use daft_io::{IOClient, IOConfig, IOStatsContext, IOStatsRef};
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_parquet::read::{
    read_parquet_bulk, read_parquet_metadata_bulk, ParquetSchemaInferenceOptions,
};
use daft_recordbatch::RecordBatch;
use daft_scan::{storage_config::StorageConfig, ChunkSpec, DataSource, ScanTask};
use daft_stats::{ColumnRangeStatistics, PartitionSpec, TableMetadata, TableStatistics};
use daft_warc::WarcConvertOptions;
use futures::{Future, Stream};
use parquet2::metadata::FileMetaData;
use snafu::ResultExt;

use crate::{DaftCSVSnafu, DaftCoreComputeSnafu};

#[derive(Debug)]
pub enum TableState {
    Unloaded(Arc<ScanTask>),
    Loaded(Arc<Vec<RecordBatch>>),
}

impl Display for TableState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unloaded(scan_task) => {
                write!(
                    f,
                    "TableState: Unloaded. To load from: {:#?}",
                    scan_task
                        .sources
                        .iter()
                        .map(daft_scan::DataSource::get_path)
                        .collect::<Vec<_>>()
                )
            }
            Self::Loaded(tables) => {
                writeln!(f, "TableState: Loaded. {} tables", tables.len())?;
                for tab in tables.iter() {
                    if !tab.is_empty() {
                        writeln!(f, "{tab}")?;
                    }
                }
                Ok(())
            }
        }
    }
}
pub type MicroPartitionRef = Arc<MicroPartition>;

#[derive(Debug)]
pub struct MicroPartition {
    /// Schema of the MicroPartition
    ///
    /// This is technically redundant with the schema in `state`:
    /// 1. If [`TableState::Loaded`]: the schema should match every underlying [`Table`]
    /// 2. If [`TableState::Unloaded`]: the schema should match the underlying [`ScanTask::materialized_schema`]
    ///
    /// However this is still useful as an easy-to-access copy of the schema, as well as to handle the corner-case
    /// of having 0 underlying [`Table`] objects (in an empty [`MicroPartition`])
    pub(crate) schema: SchemaRef,

    /// State of the MicroPartition. Can be Loaded or Unloaded.
    pub(crate) state: Mutex<TableState>,

    /// Metadata about the MicroPartition
    pub(crate) metadata: TableMetadata,

    /// Statistics about the MicroPartition
    ///
    /// If present, this must have the same [`Schema`] as [`MicroPartition::schema`], and this invariant
    /// is enforced in the `MicroPartition::new_*` constructors.
    pub(crate) statistics: Option<TableStatistics>,
}

/// Helper to run all the IO and compute required to materialize a [`ScanTask`] into a `Vec<RecordBatch>`
///
/// All [`Table`] objects returned will have the same [`Schema`] as [`ScanTask::materialized_schema`].
///
/// # Arguments
///
/// * `scan_task` - a batch of ScanTasks to materialize as Tables
/// * `io_stats` - an optional IOStats object to record the IO operations performed
fn materialize_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: Option<IOStatsRef>,
) -> crate::Result<(Vec<RecordBatch>, SchemaRef)> {
    let pushdown_columns = scan_task.pushdowns.columns.as_ref().map(|v| {
        v.iter()
            .map(std::string::String::as_str)
            .collect::<Vec<&str>>()
    });
    let file_column_names =
        get_file_column_names(pushdown_columns.as_deref(), scan_task.partition_spec());

    let urls = scan_task
        .sources
        .iter()
        .map(daft_scan::DataSource::get_path);

    let multithreaded_io = scan_task.storage_config.multithreaded_io;
    let io_config = Arc::new(
        scan_task
            .storage_config
            .io_config
            .clone()
            .unwrap_or_default(),
    );
    let io_client = daft_io::get_io_client(multithreaded_io, io_config).unwrap();

    let mut table_values = match scan_task.file_format_config.as_ref() {
        // ********************
        // Native Parquet Reads
        // ********************
        FileFormatConfig::Parquet(ParquetSourceConfig {
            coerce_int96_timestamp_unit,
            field_id_mapping,
            chunk_size,
            ..
        }) => {
            let inference_options =
                ParquetSchemaInferenceOptions::new(Some(*coerce_int96_timestamp_unit));

            // TODO: This is a hardcoded magic value but should be configurable
            let num_parallel_tasks = 8;

            let urls = urls.collect::<Vec<_>>();

            // Create vec of all unique delete files in the scan task
            let iceberg_delete_files = scan_task
                .sources
                .iter()
                .filter_map(|s| s.get_iceberg_delete_files())
                .flatten()
                .map(String::as_str)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();

            let delete_map = read_delete_files(
                iceberg_delete_files.as_slice(),
                urls.as_slice(),
                io_client.clone(),
                io_stats.clone(),
                num_parallel_tasks,
                multithreaded_io,
                &inference_options,
            )
            .context(DaftCoreComputeSnafu)?;

            let row_groups = parquet_sources_to_row_groups(scan_task.sources.as_slice());
            let metadatas = scan_task
                .sources
                .iter()
                .map(|s| s.get_parquet_metadata().cloned())
                .collect::<Option<Vec<_>>>();
            daft_parquet::read::read_parquet_bulk(
                urls.as_slice(),
                file_column_names.as_deref(),
                None,
                scan_task.pushdowns.limit,
                row_groups,
                scan_task.pushdowns.filters.clone(),
                io_client,
                io_stats,
                num_parallel_tasks,
                multithreaded_io,
                &inference_options,
                field_id_mapping.clone(),
                metadatas,
                Some(delete_map),
                *chunk_size,
            )
            .context(DaftCoreComputeSnafu)?
        }

        // ****************
        // Native CSV Reads
        // ****************
        FileFormatConfig::Csv(cfg) => {
            let schema_of_file = scan_task.schema.clone();
            let col_names = if !cfg.has_headers {
                Some(schema_of_file.field_names().collect::<Vec<_>>())
            } else {
                None
            };
            let convert_options = CsvConvertOptions::new_internal(
                scan_task.pushdowns.limit,
                file_column_names
                    .as_ref()
                    .map(|cols| cols.iter().map(|col| (*col).to_string()).collect()),
                col_names
                    .as_ref()
                    .map(|cols| cols.iter().map(|col| (*col).to_string()).collect()),
                Some(schema_of_file),
                scan_task.pushdowns.filters.clone(),
            );
            let parse_options = CsvParseOptions::new_with_defaults(
                cfg.has_headers,
                cfg.delimiter,
                cfg.double_quote,
                cfg.quote,
                cfg.allow_variable_columns,
                cfg.escape_char,
                cfg.comment,
            )
            .context(DaftCSVSnafu)?;
            let read_options = CsvReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);
            let uris = urls.collect::<Vec<_>>();
            daft_csv::read_csv_bulk(
                uris.as_slice(),
                Some(convert_options),
                Some(parse_options),
                Some(read_options),
                io_client,
                io_stats,
                scan_task.storage_config.multithreaded_io,
                None,
                8,
            )
            .context(DaftCoreComputeSnafu)?
        }

        // ****************
        // Native JSON Reads
        // ****************
        FileFormatConfig::Json(cfg) => {
            let convert_options = JsonConvertOptions::new_internal(
                scan_task.pushdowns.limit,
                file_column_names
                    .as_ref()
                    .map(|cols| cols.iter().map(|col| (*col).to_string()).collect()),
                Some(scan_task.schema.clone()),
                scan_task.pushdowns.filters.clone(),
            );
            let parse_options = JsonParseOptions::new_internal();
            let read_options = JsonReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);
            let uris = urls.collect::<Vec<_>>();
            daft_json::read_json_bulk(
                uris.as_slice(),
                Some(convert_options),
                Some(parse_options),
                Some(read_options),
                io_client,
                io_stats,
                scan_task.storage_config.multithreaded_io,
                None,
                8,
            )
            .context(DaftCoreComputeSnafu)?
        }

        // ****************
        // Native Warc Reads
        // ****************
        FileFormatConfig::Warc(_) => {
            let schema_of_file = scan_task.schema.clone();
            let convert_options = WarcConvertOptions {
                limit: scan_task.pushdowns.limit,
                include_columns: file_column_names
                    .as_ref()
                    .map(|cols| cols.iter().map(|col| (*col).to_string()).collect()),
                schema: schema_of_file,
                predicate: scan_task.pushdowns.filters.clone(),
            };
            let uris = urls.collect::<Vec<_>>();
            daft_warc::read_warc_bulk(
                uris.as_slice(),
                convert_options,
                io_client,
                io_stats,
                scan_task.storage_config.multithreaded_io,
                None,
                8,
            )
            .context(DaftCoreComputeSnafu)?
        }
        #[cfg(feature = "python")]
        FileFormatConfig::Database(DatabaseSourceConfig { sql, conn }) => {
            let predicate = scan_task
                .pushdowns
                .filters
                .as_ref()
                .map(|p| (*p.as_ref()).clone().into());
            pyo3::Python::with_gil(|py| {
                let table = crate::python::read_sql_into_py_table(
                    py,
                    sql,
                    conn,
                    predicate.clone(),
                    scan_task.schema.clone().into(),
                    scan_task
                        .pushdowns
                        .columns
                        .as_ref()
                        .map(|cols| cols.as_ref().clone()),
                    scan_task.pushdowns.limit,
                )
                .map(std::convert::Into::into)
                .context(crate::PyIOSnafu)?;
                Ok(vec![table])
            })?
        }
        #[cfg(feature = "python")]
        FileFormatConfig::PythonFunction => {
            let tables = crate::python::read_pyfunc_into_table_iter(&scan_task)?;
            tables.collect::<crate::Result<Vec<_>>>()?
        }
    };

    // Ensure that all Tables have the schema as specified by [`ScanTask::materialized_schema`]
    let cast_to_schema = scan_task.materialized_schema();

    // If there is a partition spec and partition values aren't duplicated in the data, inline the partition values
    // into the table when casting the schema.
    let fill_map = scan_task.partition_spec().map(|pspec| pspec.to_fill_map());

    table_values = table_values
        .iter()
        .map(|tbl| {
            #[allow(deprecated)]
            tbl.cast_to_schema_with_fill(cast_to_schema.as_ref(), fill_map.as_ref())
        })
        .collect::<DaftResult<Vec<_>>>()
        .context(DaftCoreComputeSnafu)?;
    Ok((table_values, cast_to_schema))
}

impl MicroPartition {
    /// Create a new "unloaded" MicroPartition using an associated [`ScanTask`]
    ///
    /// Invariants:
    /// 1. Each Loaded column statistic in `statistics` must be castable to the corresponding column in the MicroPartition's schema
    /// 2. Creating a new MicroPartition with a ScanTask that has any filter predicates or limits is not allowed and will panic
    #[must_use]
    pub fn new_unloaded(
        scan_task: Arc<ScanTask>,
        metadata: TableMetadata,
        statistics: TableStatistics,
    ) -> Self {
        assert!(scan_task.pushdowns.filters.is_none(), "Cannot create unloaded MicroPartition from a ScanTask with pushdowns that have filters");

        let schema = scan_task.materialized_schema();
        let fill_map = scan_task.partition_spec().map(|pspec| pspec.to_fill_map());
        #[allow(deprecated)]
        let statistics = statistics
            .cast_to_schema_with_fill(&schema, fill_map.as_ref())
            .expect("Statistics cannot be casted to schema");
        Self {
            schema,
            state: Mutex::new(TableState::Unloaded(scan_task)),
            metadata,
            statistics: Some(statistics),
        }
    }

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
                "Loaded MicroPartition's batch schema must match its own schema exactly"
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
            state: Mutex::new(TableState::Loaded(record_batches)),
            metadata: TableMetadata { length },
            statistics,
        }
    }

    pub fn from_arrow<S: Into<SchemaRef>>(
        schema: S,
        arrays: Vec<Box<dyn arrow2::array::Array>>,
    ) -> DaftResult<Self> {
        let schema = schema.into();
        let batch = RecordBatch::from_arrow(schema.clone(), arrays)?;
        let batches = Arc::new(vec![batch]);
        Ok(Self::new_loaded(schema, batches, None))
    }

    pub fn from_scan_task(scan_task: Arc<ScanTask>, io_stats: IOStatsRef) -> crate::Result<Self> {
        let schema = scan_task.materialized_schema();
        match (
            &scan_task.metadata,
            &scan_task.statistics,
            scan_task.file_format_config.as_ref(),
        ) {
            // CASE: ScanTask provides all required metadata.
            // If the scan_task provides metadata (e.g. retrieved from a catalog) we can use it to create an unloaded MicroPartition
            (Some(metadata), Some(statistics), _) if scan_task.pushdowns.filters.is_none() => {
                Ok(Self::new_unloaded(
                    scan_task.clone(),
                    scan_task
                        .pushdowns
                        .limit
                        .map(|limit| TableMetadata {
                            length: metadata.length.min(limit),
                        })
                        .unwrap_or_else(|| metadata.clone()),
                    statistics.clone(),
                ))
            }

            // CASE: ScanTask does not provide metadata, but the file format supports metadata retrieval
            // We can perform an eager **metadata** read to create an unloaded MicroPartition
            (
                _,
                _,
                &FileFormatConfig::Parquet(ParquetSourceConfig {
                    coerce_int96_timestamp_unit,
                    ref field_id_mapping,
                    chunk_size,
                    ..
                }),
            ) => {
                let uris = scan_task
                    .sources
                    .iter()
                    .map(daft_scan::DataSource::get_path)
                    .collect::<Vec<_>>();
                let columns = scan_task.pushdowns.columns.as_ref().map(|cols| {
                    cols.iter()
                        .map(std::string::String::as_str)
                        .collect::<Vec<&str>>()
                });
                let parquet_metadata = scan_task
                    .sources
                    .iter()
                    .map(|s| s.get_parquet_metadata().cloned())
                    .collect::<Option<Vec<_>>>();

                let row_groups = parquet_sources_to_row_groups(scan_task.sources.as_slice());

                let mut iceberg_delete_files: HashSet<&str> = HashSet::new();
                for source in &scan_task.sources {
                    if let Some(delete_files) = source.get_iceberg_delete_files() {
                        iceberg_delete_files.extend(delete_files.iter().map(String::as_str));
                    }
                }

                read_parquet_into_micropartition(
                    uris.as_slice(),
                    columns.as_deref(),
                    None,
                    scan_task.pushdowns.limit,
                    Some(iceberg_delete_files.into_iter().collect()),
                    row_groups,
                    scan_task.pushdowns.filters.clone(),
                    scan_task.partition_spec(),
                    scan_task
                        .storage_config
                        .io_config
                        .clone()
                        .map(Arc::new)
                        .unwrap_or_default(),
                    Some(io_stats),
                    if scan_task.sources.len() == 1 { 1 } else { 128 }, // Hardcoded for to 128 bulk reads
                    scan_task.storage_config.multithreaded_io,
                    &ParquetSchemaInferenceOptions {
                        coerce_int96_timestamp_unit,
                        ..Default::default()
                    },
                    Some(schema),
                    field_id_mapping.clone(),
                    parquet_metadata,
                    chunk_size,
                    scan_task.generated_fields.clone(),
                )
                .context(DaftCoreComputeSnafu)
            }

            // CASE: Last resort fallback option
            // Perform an eager **data** read
            _ => {
                let statistics = scan_task
                    .statistics
                    .clone()
                    .map(|stats| {
                        #[allow(deprecated)]
                        stats
                            .cast_to_schema(&scan_task.materialized_schema())
                            .map_err(DaftError::from)
                    })
                    .transpose()
                    .context(DaftCoreComputeSnafu)?;
                let (tables, schema) = materialize_scan_task(scan_task, Some(io_stats))?;
                Ok(Self::new_loaded(schema, Arc::new(tables), statistics))
            }
        }
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

    pub fn size_bytes(&self) -> DaftResult<Option<usize>> {
        let guard = self.state.lock().unwrap();
        let size_bytes = if let TableState::Loaded(tables) = &*guard {
            let total_size: usize = tables
                .iter()
                .map(daft_recordbatch::RecordBatch::size_bytes)
                .collect::<DaftResult<Vec<_>>>()?
                .iter()
                .sum();
            Some(total_size)
        } else if let TableState::Unloaded(scan_task) = &*guard {
            // TODO: pass in the execution config once we have it available
            scan_task.estimate_in_memory_size_bytes(None)
        } else {
            // If the table is not loaded, we don't have stats, and we don't have the file size in bytes, return None.
            // TODO(Clark): Should we pull in the table or trigger a file metadata fetch instead of returning None here?
            None
        };
        Ok(size_bytes)
    }

    /// Retrieves tables from the MicroPartition, reading data if not already loaded.
    ///
    /// This method:
    /// 1. Returns cached tables if already loaded.
    /// 2. If unloaded, reads data from the source, caches it, and returns the new tables.
    ///
    /// "Reading if necessary" means I/O operations only occur for unloaded data,
    /// optimizing performance by avoiding redundant reads.
    pub(crate) fn tables_or_read(
        &self,
        io_stats: IOStatsRef,
    ) -> crate::Result<Arc<Vec<RecordBatch>>> {
        let mut guard = self.state.lock().unwrap();
        match &*guard {
            TableState::Unloaded(scan_task) => {
                let (tables, _) = materialize_scan_task(scan_task.clone(), Some(io_stats))?;
                let table_values = Arc::new(tables);

                // Cache future accesses by setting the state to TableState::Loaded
                *guard = TableState::Loaded(table_values.clone());

                Ok(table_values)
            }
            TableState::Loaded(tables) => Ok(tables.clone()),
        }
    }

    pub fn get_tables(&self) -> crate::Result<Arc<Vec<RecordBatch>>> {
        let tables = self.tables_or_read(IOStatsContext::new("get tables"))?;
        Ok(tables)
    }

    pub fn concat_or_get(&self, io_stats: IOStatsRef) -> crate::Result<Arc<Vec<RecordBatch>>> {
        let tables = self.tables_or_read(io_stats)?;
        if tables.len() <= 1 {
            return Ok(tables);
        }

        let mut guard = self.state.lock().unwrap();

        if tables.len() > 1 {
            let new_table = RecordBatch::concat(tables.iter().collect::<Vec<_>>().as_slice())
                .context(DaftCoreComputeSnafu)?;
            *guard = TableState::Loaded(Arc::new(vec![new_table]));
        }
        if let TableState::Loaded(tables) = &*guard {
            assert_eq!(tables.len(), 1);
            Ok(tables.clone())
        } else {
            unreachable!()
        }
    }

    pub fn add_monotonically_increasing_id(
        &self,
        partition_num: u64,
        column_name: &str,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::add_monotonically_increasing_id");
        let tables = self.tables_or_read(io_stats)?;

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
        let buffer = Vec::with_capacity(self.size_bytes()?.unwrap_or(0));
        let schema = self.schema.to_arrow()?;
        let options = arrow2::io::ipc::write::WriteOptions { compression: None };
        let mut writer = arrow2::io::ipc::write::StreamWriter::new(buffer, options);
        writer.start(&schema, None)?;
        let tables = self.tables_or_read(IOStatsContext::new("write to stream"))?;
        for table in tables.iter() {
            let chunk = table.to_chunk();
            writer.write(&chunk, None)?;
        }
        writer.finish()?;
        let mut finished_buffer = writer.into_inner();
        finished_buffer.shrink_to_fit();
        Ok(finished_buffer)
    }

    pub fn read_from_ipc_stream(buffer: &[u8]) -> DaftResult<Self> {
        let mut cursor = Cursor::new(buffer);
        let stream_metadata = arrow2::io::ipc::read::read_stream_metadata(&mut cursor).unwrap();
        let schema = Arc::new(Schema::from(stream_metadata.schema.clone()));
        let reader = arrow2::io::ipc::read::StreamReader::new(cursor, stream_metadata, None);
        let tables = reader
            .into_iter()
            .map(|state| {
                let state = state?;
                let arrow_chunk = match state {
                    arrow2::io::ipc::read::StreamState::Some(chunk) => chunk,
                    _ => panic!("State should not be waiting when reading from IPC buffer"),
                };
                let record_batch =
                    RecordBatch::from_arrow(schema.clone(), arrow_chunk.into_arrays())?;
                Ok(record_batch)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Self::new_loaded(schema.into(), Arc::new(tables), None))
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
            .filter(|field| names_to_keep.contains(field.name.as_str()))
            .cloned();
        Ok(Arc::new(Schema::new(filtered_columns)))
    } else {
        Ok(schema)
    }
}

fn parquet_sources_to_row_groups(sources: &[DataSource]) -> Option<Vec<Option<Vec<i64>>>> {
    let row_groups = sources
        .iter()
        .map(|s| {
            if let Some(ChunkSpec::Parquet(row_group)) = s.get_chunk_spec() {
                Some(row_group.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if row_groups.iter().any(std::option::Option::is_some) {
        Some(row_groups)
    } else {
        None
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
            // Perform a bulk read of URIs, materializing a table per URI.
            let tables = daft_json::read_json_bulk(
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

pub fn read_warc_into_micropartition(
    uris: &[&str],
    schema: SchemaRef,
    io_config: Arc<IOConfig>,
    multithreaded_io: bool,
    io_stats: Option<IOStatsRef>,
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
    let columns: Option<&[&str]> = Some(&["file_path", "pos"]);

    let tables = read_parquet_bulk(
        delete_files,
        columns,
        None,
        None,
        None,
        None,
        io_client,
        io_stats,
        num_parallel_tasks,
        multithreaded_io,
        schema_infer_options,
        None,
        None,
        None,
        None,
    )?;

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
    let all_tables = read_parquet_bulk(
        uris,
        file_column_names.as_deref(),
        start_offset,
        num_rows,
        row_groups,
        predicate,
        io_client,
        io_stats,
        num_parallel_tasks,
        multithreaded_io,
        schema_infer_options,
        field_id_mapping,
        None,
        delete_map,
        chunk_size,
    )?;

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
    parquet_metadata: Option<Vec<Arc<FileMetaData>>>,
    chunk_size: Option<usize>,
    generated_fields: Option<SchemaRef>,
) -> DaftResult<MicroPartition> {
    if let Some(so) = start_offset
        && so > 0
    {
        return Err(common_error::DaftError::ValueError(
            "Micropartition Parquet Reader does not support non-zero start offsets".to_string(),
        ));
    }

    // Run the required I/O to retrieve all the Parquet FileMetaData
    let io_client = daft_io::get_io_client(multithreaded_io, io_config.clone())?;

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
    let (metadata, schemas) = if let Some(metadata) = parquet_metadata {
        let schemas = metadata
            .iter()
            .map(|m| {
                let schema = infer_schema_with_options(m, Some((*schema_infer_options).into()))?;
                let daft_schema = Schema::from(schema);
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
                let schema = infer_schema_with_options(m, Some((*schema_infer_options).into()))?;
                let daft_schema = schema.into();
                DaftResult::Ok(Arc::new(daft_schema))
            })
            .collect::<DaftResult<Vec<_>>>()?;
        (metadata, schemas)
    };

    let any_stats_avail = metadata
        .iter()
        .flat_map(|m| m.row_groups.values())
        .flat_map(|rg| rg.columns().iter())
        .any(|col| col.statistics().is_some());

    // If statistics are provided by the Parquet file, we create an unloaded MicroPartition
    // by constructing an appropriate ScanTask
    if any_stats_avail {
        // Prefer using the `catalog_provided_schema` but fall back onto inferred schema from Parquet files
        let scan_task_daft_schema = if let Some(catalog_provided_schema) = catalog_provided_schema {
            catalog_provided_schema
        } else {
            let unioned_schema = schemas
                .iter()
                .cloned()
                .try_reduce(|l, r| l.non_distinct_union(&r).map(Arc::new))?;
            unioned_schema.expect("we need at least 1 schema")
        };

        let stats = {
            let stat_per_table = metadata
                .iter()
                .zip(schemas.iter())
                .flat_map(|(fm, schema)| {
                    fm.row_groups
                        .values()
                        .map(|rgm| daft_parquet::row_group_metadata_to_table_stats(rgm, schema))
                })
                .collect::<DaftResult<Vec<TableStatistics>>>()?;
            stat_per_table
                .into_iter()
                .try_reduce(
                    #[allow(deprecated)]
                    |a, b| {
                        let a = a.cast_to_schema(&scan_task_daft_schema)?;
                        let b = b.cast_to_schema(&scan_task_daft_schema)?;
                        a.union(&b)
                    },
                )?
                .expect("stats should be available if any_stats_avail = true")
        };

        // Get total number of rows, accounting for selected `row_groups` and the indicated `num_rows`
        let total_rows_no_limit = match &row_groups {
            None => metadata.iter().map(|fm| fm.num_rows).sum(),
            Some(row_groups) => metadata
                .iter()
                .zip(row_groups.iter())
                .map(|(fm, rg)| match rg {
                    Some(rg) => rg
                        .iter()
                        .map(|rg_idx| fm.row_groups.get(&(*rg_idx as usize)).unwrap().num_rows())
                        .sum::<usize>(),
                    None => fm.num_rows,
                })
                .sum(),
        };
        let total_rows = num_rows.map_or(total_rows_no_limit, |num_rows| {
            num_rows.min(total_rows_no_limit)
        });

        let owned_urls = uris.iter().map(|s| (*s).to_string());
        let size_bytes = metadata
            .iter()
            .map(|m| -> u64 {
                std::iter::Sum::sum(m.row_groups.values().map(|m| m.total_byte_size() as u64))
            })
            .sum();

        let scan_task = ScanTask::new(
            owned_urls
                .into_iter()
                .zip(metadata)
                .zip(
                    row_groups
                        .clone()
                        .unwrap_or_else(|| std::iter::repeat_n(None, uris.len()).collect()),
                )
                .map(|((url, metadata), rgs)| DataSource::File {
                    path: url,
                    chunk_spec: rgs.map(ChunkSpec::Parquet),
                    size_bytes: Some(size_bytes),
                    iceberg_delete_files: None,
                    metadata: None,
                    partition_spec: partition_spec.cloned(),
                    statistics: None,
                    parquet_metadata: Some(metadata),
                })
                .collect::<Vec<_>>(),
            FileFormatConfig::Parquet(ParquetSourceConfig {
                coerce_int96_timestamp_unit: schema_infer_options.coerce_int96_timestamp_unit,
                field_id_mapping,
                row_groups,
                chunk_size,
            })
            .into(),
            scan_task_daft_schema,
            StorageConfig::new_internal(multithreaded_io, Some(io_config.as_ref().clone())).into(),
            Pushdowns::new(
                None,
                None,
                columns.map(|cols| {
                    Arc::new(
                        cols.iter()
                            .map(|v| v.as_ref().to_string())
                            .collect::<Vec<_>>(),
                    )
                }),
                num_rows,
                None,
            ),
            generated_fields,
        );

        let fill_map = scan_task.partition_spec().map(|pspec| pspec.to_fill_map());
        #[allow(deprecated)]
        let casted_stats =
            stats.cast_to_schema_with_fill(&scan_task.materialized_schema(), fill_map.as_ref())?;

        Ok(MicroPartition::new_unloaded(
            Arc::new(scan_task),
            TableMetadata { length: total_rows },
            casted_stats,
        ))
    } else {
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
}

impl Display for MicroPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.state.lock().unwrap();

        writeln!(f, "MicroPartition with {} rows:", self.len())?;

        match &*guard {
            TableState::Unloaded(..) => {
                writeln!(f, "{}\n{}", self.schema, guard)?;
            }
            TableState::Loaded(tables) => {
                if tables.is_empty() {
                    writeln!(f, "{}", self.schema)?;
                }
                writeln!(f, "{guard}")?;
            }
        }

        match &self.statistics {
            Some(t) => writeln!(f, "Statistics\n{t}")?,
            None => writeln!(f, "Statistics: missing")?,
        }

        Ok(())
    }
}

struct MicroPartitionStreamAdapter {
    state: TableState,
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
                    this.state = TableState::Loaded(tables.clone());
                    this.current = 0;
                    this.pending_task = None;
                    return Poll::Ready(tables.first().cloned().map(Ok));
                }
                Poll::Ready(Ok(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(DaftError::InternalError(e.to_string()))))
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        match &this.state {
            // if the state is unloaded, we spawn a task to load the tables
            // and set the state to loaded
            TableState::Unloaded(scan_task) => {
                let scan_task = scan_task.clone();
                let handle = tokio::spawn(async move {
                    materialize_scan_task(scan_task, None)
                        .map(|(tables, _)| tables)
                        .map_err(DaftError::from)
                });
                this.pending_task = Some(handle);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            TableState::Loaded(tables) => {
                let current = this.current;
                if current < tables.len() {
                    this.current = current + 1;
                    Poll::Ready(tables.get(current).cloned().map(Ok))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}
impl MicroPartition {
    pub fn into_stream(self: Arc<Self>) -> DaftResult<impl Stream<Item = DaftResult<RecordBatch>>> {
        let state = match &*self.state.lock().unwrap() {
            TableState::Unloaded(scan_task) => TableState::Unloaded(scan_task.clone()),
            TableState::Loaded(tables) => TableState::Loaded(tables.clone()),
        };

        Ok(MicroPartitionStreamAdapter {
            state,
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
        datatypes::{DataType, Field, Int32Array},
        prelude::Schema,
        series::IntoSeries,
    };
    use daft_recordbatch::RecordBatch;
    use futures::StreamExt;

    use crate::MicroPartition;

    #[tokio::test]
    async fn test_mp_stream() -> DaftResult<()> {
        let columns = vec![Int32Array::from_values("a", vec![1].into_iter()).into_series()];
        let columns2 = vec![Int32Array::from_values("a", vec![2].into_iter()).into_series()];
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
}
