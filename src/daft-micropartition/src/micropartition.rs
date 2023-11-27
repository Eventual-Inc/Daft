use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use std::{ops::Deref, sync::Mutex};

use arrow2::io::parquet::read::schema::infer_schema_with_options;
use common_error::DaftResult;
use daft_core::schema::{Schema, SchemaRef};

use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_parquet::read::{
    read_parquet_bulk, read_parquet_metadata_bulk, ParquetSchemaInferenceOptions,
};
use daft_scan::file_format::{CsvSourceConfig, FileFormatConfig, ParquetSourceConfig};
use daft_scan::storage_config::{NativeStorageConfig, StorageConfig};
use daft_scan::{ChunkSpec, DataFileSource, Pushdowns, ScanTask};
use daft_table::Table;

use snafu::ResultExt;

#[cfg(feature = "python")]
use crate::PyIOSnafu;
use crate::{DaftCSVSnafu, DaftCoreComputeSnafu};

use daft_io::{IOConfig, IOStatsRef};
use daft_stats::TableMetadata;
use daft_stats::TableStatistics;

pub(crate) enum TableState {
    Unloaded(Arc<ScanTask>),
    Loaded(Arc<Vec<Table>>),
}

impl Display for TableState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableState::Unloaded(scan_task) => {
                write!(
                    f,
                    "TableState: Unloaded. To load from: {:#?}",
                    scan_task
                        .sources
                        .iter()
                        .map(|s| s.get_path())
                        .collect::<Vec<_>>()
                )
            }
            TableState::Loaded(tables) => {
                writeln!(f, "TableState: Loaded. {} tables", tables.len())?;
                for tab in tables.iter() {
                    writeln!(f, "{}", tab)?;
                }
                Ok(())
            }
        }
    }
}
pub(crate) struct MicroPartition {
    pub(crate) schema: SchemaRef,
    pub(crate) state: Mutex<TableState>,
    pub(crate) metadata: TableMetadata,
    pub(crate) statistics: Option<TableStatistics>,
}

/// Helper to run all the IO and compute required to materialize a ScanTask into a Vec<Table>
///
/// # Arguments
///
/// * `scan_task` - a batch of ScanTasks to materialize as Tables
/// * `cast_to_schema` - an Optional schema to cast all the resulting Tables to. If not provided, will use the schema
///     provided by the ScanTask
/// * `io_stats` - an optional IOStats object to record the IO operations performed
fn materialize_scan_task(
    scan_task: Arc<ScanTask>,
    cast_to_schema: Option<SchemaRef>,
    io_stats: Option<IOStatsRef>,
) -> crate::Result<(Vec<Table>, SchemaRef)> {
    log::debug!("Materializing ScanTask: {scan_task:?}");

    let column_names = scan_task
        .pushdowns
        .columns
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_ref()).collect::<Vec<&str>>());
    let urls = scan_task.sources.iter().map(|s| s.get_path());

    // Schema to cast resultant tables into, ensuring that all Tables have the same schema.
    // Note that we need to apply column pruning here if specified by the ScanTask
    let cast_to_schema = cast_to_schema.unwrap_or_else(|| scan_task.schema.clone());

    let table_values = match scan_task.storage_config.as_ref() {
        StorageConfig::Native(native_storage_config) => {
            let runtime_handle =
                daft_io::get_runtime(native_storage_config.multithreaded_io).unwrap();
            let io_config = Arc::new(
                native_storage_config
                    .io_config
                    .as_ref()
                    .cloned()
                    .unwrap_or_default(),
            );
            let io_client =
                daft_io::get_io_client(native_storage_config.multithreaded_io, io_config).unwrap();

            match scan_task.file_format_config.as_ref() {
                // ********************
                // Native Parquet Reads
                // ********************
                FileFormatConfig::Parquet(ParquetSourceConfig {
                    coerce_int96_timestamp_unit,
                }) => {
                    let inference_options =
                        ParquetSchemaInferenceOptions::new(Some(*coerce_int96_timestamp_unit));
                    let urls = urls.collect::<Vec<_>>();
                    let row_groups = parquet_sources_to_row_groups(scan_task.sources.as_slice());
                    daft_parquet::read::read_parquet_bulk(
                        urls.as_slice(),
                        column_names.as_deref(),
                        None,
                        scan_task.pushdowns.limit,
                        row_groups,
                        io_client.clone(),
                        io_stats,
                        8,
                        runtime_handle,
                        &inference_options,
                    )
                    .context(DaftCoreComputeSnafu)?
                }

                // ****************
                // Native CSV Reads
                // ****************
                FileFormatConfig::Csv(cfg @ CsvSourceConfig { .. }) => {
                    let col_names = if !cfg.has_headers {
                        Some(
                            cast_to_schema
                                .fields
                                .values()
                                .map(|f| f.name.as_str())
                                .collect::<Vec<_>>(),
                        )
                    } else {
                        None
                    };
                    let convert_options = CsvConvertOptions::new_internal(
                        scan_task.pushdowns.limit,
                        column_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
                        col_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
                        None,
                    );
                    let parse_options = CsvParseOptions::new_with_defaults(
                        cfg.has_headers,
                        cfg.delimiter,
                        cfg.double_quote,
                        cfg.quote,
                        cfg.escape_char,
                        cfg.comment,
                    )
                    .context(DaftCSVSnafu)?;
                    let read_options =
                        CsvReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);
                    let uris = urls.collect::<Vec<_>>();
                    daft_csv::read_csv_bulk(
                        uris.as_slice(),
                        Some(convert_options),
                        Some(parse_options),
                        Some(read_options),
                        io_client,
                        io_stats,
                        native_storage_config.multithreaded_io,
                        None,
                        8,
                    )
                    .context(DaftCoreComputeSnafu)?
                }

                // ****************
                // Native JSON Reads
                // ****************
                FileFormatConfig::Json(_) => {
                    todo!("TODO: Implement MicroPartition native reads for JSON.");
                }
            }
        }
        #[cfg(feature = "python")]
        StorageConfig::Python(_) => {
            use pyo3::Python;
            match scan_task.file_format_config.as_ref() {
                FileFormatConfig::Parquet(ParquetSourceConfig {
                    coerce_int96_timestamp_unit,
                    ..
                }) => Python::with_gil(|py| {
                    urls.map(|url| {
                        crate::python::read_parquet_into_py_table(
                            py,
                            url,
                            cast_to_schema.clone().into(),
                            (*coerce_int96_timestamp_unit).into(),
                            scan_task.storage_config.clone().into(),
                            scan_task
                                .pushdowns
                                .columns
                                .as_ref()
                                .map(|cols| cols.as_ref().clone()),
                            scan_task.pushdowns.limit,
                        )
                        .map(|t| t.into())
                        .context(PyIOSnafu)
                    })
                    .collect::<crate::Result<Vec<_>>>()
                })?,
                FileFormatConfig::Csv(CsvSourceConfig {
                    has_headers,
                    delimiter,
                    double_quote,
                    ..
                }) => Python::with_gil(|py| {
                    urls.map(|url| {
                        crate::python::read_csv_into_py_table(
                            py,
                            url,
                            *has_headers,
                            *delimiter,
                            *double_quote,
                            cast_to_schema.clone().into(),
                            scan_task.storage_config.clone().into(),
                            scan_task
                                .pushdowns
                                .columns
                                .as_ref()
                                .map(|cols| cols.as_ref().clone()),
                            scan_task.pushdowns.limit,
                        )
                        .map(|t| t.into())
                        .context(PyIOSnafu)
                    })
                    .collect::<crate::Result<Vec<_>>>()
                })?,
                FileFormatConfig::Json(_) => Python::with_gil(|py| {
                    urls.map(|url| {
                        crate::python::read_json_into_py_table(
                            py,
                            url,
                            cast_to_schema.clone().into(),
                            scan_task.storage_config.clone().into(),
                            scan_task
                                .pushdowns
                                .columns
                                .as_ref()
                                .map(|cols| cols.as_ref().clone()),
                            scan_task.pushdowns.limit,
                        )
                        .map(|t| t.into())
                        .context(PyIOSnafu)
                    })
                    .collect::<crate::Result<Vec<_>>>()
                })?,
            }
        }
    };
    let cast_to_schema = prune_fields_from_schema_ref(cast_to_schema, column_names.as_deref())
        .context(DaftCoreComputeSnafu)?;

    let casted_table_values = table_values
        .iter()
        .map(|tbl| tbl.cast_to_schema(cast_to_schema.as_ref()))
        .collect::<DaftResult<Vec<_>>>()
        .context(DaftCoreComputeSnafu)?;
    Ok((casted_table_values, cast_to_schema))
}

impl MicroPartition {
    /// Create a new "unloaded" MicroPartition using an associated [`ScanTask`]
    ///
    /// Schema invariants:
    /// 1. All columns in `schema` must be exist in the `scan_task` schema
    /// 2. Each Loaded column statistic in `statistics` must be castable to the corresponding column in the MicroPartition's schema
    pub fn new_unloaded(
        schema: SchemaRef,
        scan_task: Arc<ScanTask>,
        metadata: TableMetadata,
        statistics: TableStatistics,
    ) -> Self {
        assert!(
            schema
                .fields
                .keys()
                .collect::<HashSet<_>>()
                .is_subset(&scan_task.schema.fields.keys().collect::<HashSet<_>>()),
            "Unloaded MicroPartition's schema names must be a subset of its ScanTask's schema"
        );

        MicroPartition {
            schema: schema.clone(),
            state: Mutex::new(TableState::Unloaded(scan_task)),
            metadata,
            statistics: Some(
                statistics
                    .cast_to_schema(schema)
                    .expect("Statistics cannot be casted to schema"),
            ),
        }
    }

    /// Create a new "loaded" MicroPartition using the materialized tables
    ///
    /// Schema invariants:
    /// 1. `schema` must match each Table's schema exactly
    /// 2. If `statistics` is provided, each Loaded column statistic must be castable to the corresponding column in the MicroPartition's schema
    pub fn new_loaded(
        schema: SchemaRef,
        tables: Arc<Vec<Table>>,
        statistics: Option<TableStatistics>,
    ) -> Self {
        // Check and validate invariants with asserts
        for table in tables.iter() {
            assert!(
                table.schema == schema,
                "Loaded MicroPartition's tables' schema must match its own schema exactly"
            );
        }

        let statistics = statistics.map(|stats| {
            stats
                .cast_to_schema(schema.clone())
                .expect("Statistics cannot be casted to schema")
        });
        let tables_len_sum = tables.iter().map(|t| t.len()).sum();

        MicroPartition {
            schema,
            state: Mutex::new(TableState::Loaded(tables)),
            metadata: TableMetadata {
                length: tables_len_sum,
            },
            statistics,
        }
    }

    pub fn from_scan_task(scan_task: Arc<ScanTask>, io_stats: IOStatsRef) -> crate::Result<Self> {
        let schema = scan_task.schema.clone();
        match (
            &scan_task.metadata,
            &scan_task.statistics,
            scan_task.file_format_config.as_ref(),
            scan_task.storage_config.as_ref(),
        ) {
            // CASE: ScanTask provides all required metadata.
            // If the scan_task provides metadata (e.g. retrieved from a catalog) we can use it to create an unloaded MicroPartition
            (Some(metadata), Some(statistics), _, _) => Ok(Self::new_unloaded(
                schema,
                scan_task.clone(),
                metadata.clone(),
                statistics.clone(),
            )),

            // CASE: ScanTask does not provide metadata, but the file format supports metadata retrieval
            // We can perform an eager **metadata** read to create an unloaded MicroPartition
            (
                _,
                _,
                FileFormatConfig::Parquet(ParquetSourceConfig {
                    coerce_int96_timestamp_unit,
                }),
                StorageConfig::Native(cfg),
            ) => {
                let uris = scan_task
                    .sources
                    .iter()
                    .map(|s| s.get_path())
                    .collect::<Vec<_>>();
                let columns = scan_task
                    .pushdowns
                    .columns
                    .as_ref()
                    .map(|cols| cols.iter().map(|s| s.as_str()).collect::<Vec<&str>>());

                let row_groups = parquet_sources_to_row_groups(scan_task.sources.as_slice());

                read_parquet_into_micropartition(
                    uris.as_slice(),
                    columns.as_deref(),
                    None,
                    scan_task.pushdowns.limit,
                    row_groups,
                    cfg.io_config
                        .clone()
                        .map(|c| Arc::new(c.clone()))
                        .unwrap_or_default(),
                    Some(io_stats),
                    if scan_task.sources.len() == 1 { 1 } else { 128 }, // Hardcoded for to 128 bulk reads
                    cfg.multithreaded_io,
                    &ParquetSchemaInferenceOptions {
                        coerce_int96_timestamp_unit: *coerce_int96_timestamp_unit,
                    },
                )
                .context(DaftCoreComputeSnafu)
            }

            // CASE: Last resort fallback option
            // Perform an eager **data** read
            _ => {
                let statistics = scan_task.statistics.clone();
                let (tables, schema) = materialize_scan_task(scan_task, None, Some(io_stats))?;
                Ok(Self::new_loaded(schema, Arc::new(tables), statistics))
            }
        }
    }

    pub fn empty(schema: Option<SchemaRef>) -> Self {
        let schema = schema.unwrap_or(Schema::empty().into());
        Self::new_loaded(schema, Arc::new(vec![]), None)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.schema.names()
    }

    pub fn len(&self) -> usize {
        self.metadata.length
    }

    pub fn size_bytes(&self) -> DaftResult<Option<usize>> {
        let guard = self.state.lock().unwrap();
        let size_bytes = if let TableState::Loaded(tables) = guard.deref() {
            let total_size: usize = tables
                .iter()
                .map(|t| t.size_bytes())
                .collect::<DaftResult<Vec<_>>>()?
                .iter()
                .sum();
            Some(total_size)
        } else if let Some(stats) = &self.statistics {
            let row_size = stats.estimate_row_size()?;
            Some(row_size * self.len())
        } else if let TableState::Unloaded(scan_task) = guard.deref() && let Some(size_bytes_on_disk) = scan_task.size_bytes_on_disk {
            Some(size_bytes_on_disk as usize)
        } else {
            // If the table is not loaded, we don't have stats, and we don't have the file size in bytes, return None.
            // TODO(Clark): Should we pull in the table or trigger a file metadata fetch instead of returning None here?
            None
        };
        Ok(size_bytes)
    }

    pub(crate) fn tables_or_read(&self, io_stats: IOStatsRef) -> crate::Result<Arc<Vec<Table>>> {
        let mut guard = self.state.lock().unwrap();
        match guard.deref() {
            TableState::Unloaded(scan_task) => {
                let (tables, _) = materialize_scan_task(
                    scan_task.clone(),
                    Some(self.schema.clone()),
                    Some(io_stats),
                )?;
                let table_values = Arc::new(tables);

                // Cache future accesses by setting the state to TableState::Loaded
                *guard = TableState::Loaded(table_values.clone());

                Ok(table_values)
            }
            TableState::Loaded(tables) => Ok(tables.clone()),
        }
    }

    pub(crate) fn concat_or_get(&self, io_stats: IOStatsRef) -> crate::Result<Arc<Vec<Table>>> {
        let tables = self.tables_or_read(io_stats)?;
        if tables.len() <= 1 {
            return Ok(tables);
        }

        let mut guard = self.state.lock().unwrap();

        if tables.len() > 1 {
            let new_table = Table::concat(tables.iter().collect::<Vec<_>>().as_slice())
                .context(DaftCoreComputeSnafu)?;
            *guard = TableState::Loaded(Arc::new(vec![new_table]));
        };
        if let TableState::Loaded(tables) = guard.deref() {
            assert_eq!(tables.len(), 1);
            Ok(tables.clone())
        } else {
            unreachable!()
        }
    }
}

fn prune_fields_from_schema(schema: Schema, columns: Option<&[&str]>) -> DaftResult<Schema> {
    if let Some(columns) = columns {
        let avail_names = schema
            .fields
            .keys()
            .map(|f| f.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name) {
                names_to_keep.insert(*col_name);
            } else {
                return Err(super::Error::FieldNotFound {
                    field: col_name.to_string(),
                    available_fields: avail_names.iter().map(|v| v.to_string()).collect(),
                }
                .into());
            }
        }
        let filtered_columns = schema
            .fields
            .into_values()
            .filter(|field| names_to_keep.contains(field.name.as_str()))
            .collect::<Vec<_>>();
        Schema::new(filtered_columns)
    } else {
        Ok(schema)
    }
}

fn prune_fields_from_schema_ref(
    schema: SchemaRef,
    columns: Option<&[&str]>,
) -> DaftResult<SchemaRef> {
    if let Some(columns) = columns {
        let avail_names = schema
            .fields
            .keys()
            .map(|f| f.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns.iter() {
            if avail_names.contains(col_name) {
                names_to_keep.insert(*col_name);
            } else {
                return Err(super::Error::FieldNotFound {
                    field: col_name.to_string(),
                    available_fields: avail_names.iter().map(|v| v.to_string()).collect(),
                }
                .into());
            }
        }
        let filtered_columns = schema
            .fields
            .values()
            .filter(|field| names_to_keep.contains(field.name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        Ok(Schema::new(filtered_columns)?.into())
    } else {
        Ok(schema)
    }
}

fn parquet_sources_to_row_groups(sources: &[DataFileSource]) -> Option<Vec<Option<Vec<i64>>>> {
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
    if row_groups.iter().any(|rgs| rgs.is_some()) {
        Some(row_groups)
    } else {
        None
    }
}

pub(crate) fn read_csv_into_micropartition(
    uris: &[&str],
    convert_options: Option<CsvConvertOptions>,
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    io_config: Arc<IOConfig>,
    multithreaded_io: bool,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<MicroPartition> {
    let io_client = daft_io::get_io_client(multithreaded_io, io_config.clone())?;

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
                .try_reduce(|s1, s2| s1.union(s2.as_ref()).map(Arc::new))?
                .unwrap();
            let tables = tables
                .into_iter()
                .map(|tbl| tbl.cast_to_schema(&unioned_schema))
                .collect::<DaftResult<Vec<_>>>()?;

            // Construct MicroPartition from tables and unioned schema
            Ok(MicroPartition::new_loaded(
                unioned_schema.clone(),
                Arc::new(tables),
                None,
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn read_parquet_into_micropartition(
    uris: &[&str],
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<Option<Vec<i64>>>>,
    io_config: Arc<IOConfig>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    multithreaded_io: bool,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<MicroPartition> {
    if let Some(so) = start_offset && so > 0 {
        return Err(common_error::DaftError::ValueError("Micropartition Parquet Reader does not support non-zero start offsets".to_string()));
    }

    // Run the required I/O to retrieve all the Parquet FileMetaData
    let runtime_handle = daft_io::get_runtime(multithreaded_io)?;
    let io_client = daft_io::get_io_client(multithreaded_io, io_config.clone())?;
    let meta_io_client = io_client.clone();
    let meta_io_stats = io_stats.clone();
    let metadata = runtime_handle.block_on(async move {
        read_parquet_metadata_bulk(uris, meta_io_client, meta_io_stats).await
    })?;

    // Deserialize and collect relevant TableStatistics
    let schemas = metadata
        .iter()
        .map(|m| {
            let schema = infer_schema_with_options(m, &Some((*schema_infer_options).into()))?;
            let daft_schema = daft_core::schema::Schema::try_from(&schema)?;
            DaftResult::Ok(daft_schema)
        })
        .collect::<DaftResult<Vec<_>>>()?;
    let any_stats_avail = metadata
        .iter()
        .flat_map(|m| m.row_groups.iter())
        .flat_map(|rg| rg.columns().iter())
        .any(|col| col.statistics().is_some());
    let stats = if any_stats_avail {
        let stat_per_table = metadata
            .iter()
            .zip(schemas.iter())
            .flat_map(|(fm, schema)| {
                fm.row_groups
                    .iter()
                    .map(|rgm| daft_parquet::row_group_metadata_to_table_stats(rgm, schema))
            })
            .collect::<DaftResult<Vec<TableStatistics>>>()?;
        stat_per_table.into_iter().try_reduce(|a, b| a.union(&b))?
    } else {
        None
    };

    // Union and prune the schema using the specified `columns`
    let unioned_schema = schemas.into_iter().try_reduce(|l, r| l.union(&r))?;
    let full_daft_schema = unioned_schema.expect("we need at least 1 schema");
    let pruned_daft_schema = prune_fields_from_schema(full_daft_schema, columns)?;

    // Get total number of rows, accounting for selected `row_groups` and the indicated `num_rows`
    let total_rows_no_limit = match &row_groups {
        None => metadata.iter().map(|fm| fm.num_rows).sum(),
        Some(row_groups) => metadata
            .iter()
            .zip(row_groups.iter())
            .map(|(fm, rg)| match rg {
                Some(rg) => rg
                    .iter()
                    .map(|rg_idx| fm.row_groups.get(*rg_idx as usize).unwrap().num_rows())
                    .sum::<usize>(),
                None => fm.num_rows,
            })
            .sum(),
    };
    let total_rows = num_rows
        .map(|num_rows| num_rows.min(total_rows_no_limit))
        .unwrap_or(total_rows_no_limit);

    if let Some(stats) = stats {
        let owned_urls = uris.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        let daft_schema = Arc::new(pruned_daft_schema);
        let size_bytes = metadata
            .iter()
            .map(|m| -> u64 {
                std::iter::Sum::sum(m.row_groups.iter().map(|m| m.total_byte_size() as u64))
            })
            .sum();
        let scan_task = ScanTask::new(
            owned_urls
                .into_iter()
                .zip(
                    row_groups
                        .unwrap_or_else(|| std::iter::repeat(None).take(uris.len()).collect()),
                )
                .map(|(url, rgs)| DataFileSource::AnonymousDataFile {
                    path: url,
                    chunk_spec: rgs.map(ChunkSpec::Parquet),
                    size_bytes: Some(size_bytes),
                    metadata: None,
                    partition_spec: None,
                    statistics: None,
                })
                .collect::<Vec<_>>(),
            FileFormatConfig::Parquet(ParquetSourceConfig {
                coerce_int96_timestamp_unit: schema_infer_options.coerce_int96_timestamp_unit,
            })
            .into(),
            daft_schema.clone(),
            StorageConfig::Native(
                NativeStorageConfig::new_internal(
                    multithreaded_io,
                    Some(io_config.as_ref().clone()),
                )
                .into(),
            )
            .into(),
            Pushdowns::new(
                None,
                columns
                    .map(|cols| Arc::new(cols.iter().map(|v| v.to_string()).collect::<Vec<_>>())),
                num_rows,
            ),
        );

        let exprs = daft_schema
            .fields
            .keys()
            .map(|n| daft_dsl::col(n.as_str()))
            .collect::<Vec<_>>();
        // use schema to update stats
        let stats = stats.eval_expression_list(exprs.as_slice(), daft_schema.as_ref())?;

        Ok(MicroPartition::new_unloaded(
            scan_task.schema.clone(),
            Arc::new(scan_task),
            TableMetadata { length: total_rows },
            stats,
        ))
    } else {
        let all_tables = read_parquet_bulk(
            uris,
            columns,
            start_offset,
            num_rows,
            row_groups,
            io_client,
            io_stats,
            num_parallel_tasks,
            runtime_handle,
            schema_infer_options,
        )?;
        let all_tables = all_tables
            .into_iter()
            .map(|t| t.cast_to_schema(&pruned_daft_schema))
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(MicroPartition::new_loaded(
            Arc::new(pruned_daft_schema),
            all_tables.into(),
            None,
        ))
    }
}

impl Display for MicroPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.state.lock().unwrap();

        writeln!(f, "MicroPartition with {} rows:", self.len())?;

        match guard.deref() {
            TableState::Unloaded(..) => {
                writeln!(f, "{}\n{}", self.schema, guard)?;
            }
            TableState::Loaded(tables) => {
                if tables.len() == 0 {
                    writeln!(f, "{}", self.schema)?;
                }
                writeln!(f, "{}", guard)?;
            }
        };

        match &self.statistics {
            Some(t) => writeln!(f, "Statistics\n{}", t)?,
            None => writeln!(f, "Statistics: missing")?,
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {}
