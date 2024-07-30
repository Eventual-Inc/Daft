use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;
use std::{ops::Deref, sync::Mutex};

use arrow2::io::parquet::read::schema::infer_schema_with_options;
use common_error::DaftResult;
use daft_core::datatypes::{Field, Int64Array, Utf8Array};
use daft_core::schema::{Schema, SchemaRef};

use daft_core::DataType;
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_dsl::ExprRef;
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_parquet::read::{
    read_parquet_bulk, read_parquet_metadata_bulk, ParquetSchemaInferenceOptions,
};
use daft_scan::file_format::{CsvSourceConfig, FileFormatConfig, ParquetSourceConfig};
use daft_scan::storage_config::{NativeStorageConfig, StorageConfig};
use daft_scan::{ChunkSpec, DataSource, Pushdowns, ScanTask};
use daft_table::Table;

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use parquet2::metadata::FileMetaData;
use snafu::ResultExt;

#[cfg(feature = "python")]
use crate::PyIOSnafu;
use crate::{DaftCSVSnafu, DaftCoreComputeSnafu};

use daft_io::{IOClient, IOConfig, IOStatsContext, IOStatsRef};
use daft_stats::TableStatistics;
use daft_stats::{PartitionSpec, TableMetadata};

#[derive(Debug)]
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

/// Helper to run all the IO and compute required to materialize a [`ScanTask`] into a `Vec<Table>`
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
) -> crate::Result<(Vec<Table>, SchemaRef)> {
    let pushdown_columns = scan_task
        .pushdowns
        .columns
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<&str>>());
    let file_column_names =
        _get_file_column_names(pushdown_columns.as_deref(), scan_task.partition_spec());

    let urls = scan_task.sources.iter().map(|s| s.get_path());

    let mut table_values = match scan_task.storage_config.as_ref() {
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
                    field_id_mapping,
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
                        .flat_map(|s| s.get_iceberg_delete_files())
                        .flatten()
                        .map(String::as_str)
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>();

                    let delete_map = _read_delete_files(
                        iceberg_delete_files.as_slice(),
                        urls.as_slice(),
                        io_client.clone(),
                        io_stats.clone(),
                        num_parallel_tasks,
                        runtime_handle.clone(),
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
                        io_client.clone(),
                        io_stats,
                        num_parallel_tasks,
                        runtime_handle,
                        &inference_options,
                        field_id_mapping.clone(),
                        metadatas,
                        Some(delete_map),
                    )
                    .context(DaftCoreComputeSnafu)?
                }

                // ****************
                // Native CSV Reads
                // ****************
                FileFormatConfig::Csv(cfg) => {
                    let schema_of_file = scan_task.schema.clone();
                    let col_names = if !cfg.has_headers {
                        Some(
                            schema_of_file
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
                        file_column_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
                        col_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
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
                FileFormatConfig::Json(cfg) => {
                    let convert_options = JsonConvertOptions::new_internal(
                        scan_task.pushdowns.limit,
                        file_column_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
                        Some(scan_task.schema.clone()),
                        scan_task.pushdowns.filters.clone(),
                    );
                    let parse_options = JsonParseOptions::new_internal();
                    let read_options =
                        JsonReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);
                    let uris = urls.collect::<Vec<_>>();
                    daft_json::read_json_bulk(
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
                #[cfg(feature = "python")]
                FileFormatConfig::Database(_) => {
                    return Err(common_error::DaftError::TypeError(
                        "Native reads for Database file format not implemented".to_string(),
                    ))
                    .context(DaftCoreComputeSnafu);
                }
                #[cfg(feature = "python")]
                FileFormatConfig::PythonFunction => {
                    return Err(common_error::DaftError::TypeError(
                        "Native reads for PythonFunction file format not implemented".to_string(),
                    ))
                    .context(DaftCoreComputeSnafu);
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
                            scan_task.schema.clone().into(),
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
                            scan_task.schema.clone().into(),
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
                            scan_task.schema.clone().into(),
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
                FileFormatConfig::Database(daft_scan::file_format::DatabaseSourceConfig {
                    sql,
                    conn,
                }) => {
                    let predicate = scan_task
                        .pushdowns
                        .filters
                        .as_ref()
                        .map(|p| (*p.as_ref()).clone().into());
                    Python::with_gil(|py| {
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
                        .map(|t| t.into())
                        .context(PyIOSnafu)?;
                        Ok(vec![table])
                    })?
                }
                FileFormatConfig::PythonFunction => {
                    use pyo3::ToPyObject;

                    let table_iterators = scan_task.sources.iter().map(|source| {
                        // Call Python function to create an Iterator (Grabs the GIL and then releases it)
                        match source {
                            DataSource::PythonFactoryFunction {
                                module,
                                func_name,
                                func_args,
                                ..
                            } => {
                                Python::with_gil(|py| {
                                    let func = py.import(pyo3::types::PyString::new(py, module)).unwrap_or_else(|_| panic!("Cannot import factory function from module {module}")).getattr(pyo3::types::PyString::new(py, func_name)).unwrap_or_else(|_| panic!("Cannot find function {func_name} in module {module}"));
                                    Ok(func.call(func_args.to_pytuple(py), None).with_context(|_| PyIOSnafu)?.downcast::<pyo3::types::PyIterator>().expect("Function must return an iterator of tables")).map(|it| it.to_object(py))
                                })
                            },
                            _ => unreachable!("PythonFunction file format must be paired with PythonFactoryFunction data file sources"),
                        }
                    });

                    let mut tables = Vec::new();
                    let mut rows_seen_so_far = 0;
                    for iterator in table_iterators {
                        let iterator = iterator?;

                        // Iterate on this iterator to exhaustion, or until the limit is met
                        while scan_task
                            .pushdowns
                            .limit
                            .map(|limit| rows_seen_so_far < limit)
                            .unwrap_or(true)
                        {
                            // Grab the GIL to call next() on the iterator, and then release it once we have the Table
                            let table = match Python::with_gil(|py| {
                                iterator
                                    .downcast::<pyo3::types::PyIterator>(py)
                                    .unwrap()
                                    .next()
                                    .map(|result| {
                                        result
                                            .map(|tbl| {
                                                tbl.extract::<daft_table::python::PyTable>()
                                                    .expect("Must be a PyTable")
                                                    .table
                                            })
                                            .with_context(|_| PyIOSnafu)
                                    })
                            }) {
                                Some(table) => table,
                                None => break,
                            }?;

                            // Apply filters
                            let table = if let Some(filters) = scan_task.pushdowns.filters.as_ref()
                            {
                                table
                                    .filter(&[filters.clone()])
                                    .with_context(|_| DaftCoreComputeSnafu)?
                            } else {
                                table
                            };

                            // Apply limit if necessary, and update `&mut remaining`
                            let table = if let Some(limit) = scan_task.pushdowns.limit {
                                let limited_table = if rows_seen_so_far + table.len() > limit {
                                    table
                                        .slice(0, limit - rows_seen_so_far)
                                        .with_context(|_| DaftCoreComputeSnafu)?
                                } else {
                                    table
                                };

                                // Update the rows_seen_so_far
                                rows_seen_so_far += limited_table.len();

                                limited_table
                            } else {
                                table
                            };

                            tables.push(table);
                        }

                        // If seen enough rows, early-terminate
                        if scan_task
                            .pushdowns
                            .limit
                            .map(|limit| rows_seen_so_far >= limit)
                            .unwrap_or(false)
                        {
                            break;
                        }
                    }

                    tables
                }
            }
        }
    };

    // Ensure that all Tables have the schema as specified by [`ScanTask::materialized_schema`]
    let cast_to_schema = scan_task.materialized_schema();

    // If there is a partition spec and partition values aren't duplicated in the data, inline the partition values
    // into the table when casting the schema.
    let fill_map = scan_task.partition_spec().map(|pspec| pspec.to_fill_map());

    table_values = table_values
        .iter()
        .map(|tbl| tbl.cast_to_schema_with_fill(cast_to_schema.as_ref(), fill_map.as_ref()))
        .collect::<DaftResult<Vec<_>>>()
        .context(DaftCoreComputeSnafu)?;
    Ok((table_values, cast_to_schema))
}

async fn stream_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: Option<IOStatsRef>,
) -> crate::Result<BoxStream<'static, DaftResult<Vec<Table>>>> {
    let pushdown_columns = scan_task
        .pushdowns
        .columns
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<&str>>());
    let file_column_names =
        _get_file_column_names(pushdown_columns.as_deref(), scan_task.partition_spec());

    if scan_task.sources.len() != 1 {
        return Err(common_error::DaftError::TypeError(
            "Streaming reads only supported for single source ScanTasks".to_string(),
        ))
        .context(DaftCoreComputeSnafu);
    }
    let url = scan_task.sources.first().map(|s| s.get_path()).unwrap();
    let table_values = match scan_task.storage_config.as_ref() {
        StorageConfig::Native(native_storage_config) => {
            let io_config = Arc::new(
                native_storage_config
                    .io_config
                    .as_ref()
                    .cloned()
                    .unwrap_or_default(),
            );
            let multi_threaded_io = native_storage_config.multithreaded_io;
            let io_client = daft_io::get_io_client_async(multi_threaded_io, io_config)
                .await
                .unwrap();

            match scan_task.file_format_config.as_ref() {
                // ********************
                // Native Parquet Reads
                // ********************
                FileFormatConfig::Parquet(ParquetSourceConfig {
                    coerce_int96_timestamp_unit: _,
                    field_id_mapping: _,
                    ..
                }) => {
                    todo!("Implement streaming reads for Parquet")
                }

                // ****************
                // Native CSV Reads
                // ****************
                FileFormatConfig::Csv(cfg) => {
                    let schema_of_file = scan_task.schema.clone();
                    let col_names = if !cfg.has_headers {
                        Some(
                            schema_of_file
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
                        file_column_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
                        col_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
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
                    let read_options =
                        CsvReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);
                    daft_csv::stream_csv(
                        url.to_string(),
                        Some(convert_options),
                        Some(parse_options),
                        Some(read_options),
                        io_client.clone(),
                        io_stats.clone(),
                        None,
                    )
                    .await
                    .context(DaftCoreComputeSnafu)?
                }

                // ****************
                // Native JSON Reads
                // ****************
                FileFormatConfig::Json(cfg) => {
                    let schema_of_file = scan_task.schema.clone();
                    let convert_options = JsonConvertOptions::new_internal(
                        scan_task.pushdowns.limit,
                        file_column_names
                            .as_ref()
                            .map(|cols| cols.iter().map(|col| col.to_string()).collect()),
                        Some(schema_of_file),
                        scan_task.pushdowns.filters.clone(),
                    );
                    // let
                    let parse_options = JsonParseOptions::new_internal();
                    let read_options =
                        JsonReadOptions::new_internal(cfg.buffer_size, cfg.chunk_size);

                    daft_json::read::stream_json(
                        url.to_string(),
                        Some(convert_options),
                        Some(parse_options),
                        Some(read_options),
                        io_client,
                        io_stats,
                        None,
                    )
                    .await
                    .context(DaftCoreComputeSnafu)?
                }
                #[cfg(feature = "python")]
                FileFormatConfig::Database(_) => {
                    return Err(common_error::DaftError::TypeError(
                        "Native reads for Database file format not implemented".to_string(),
                    ))
                    .context(DaftCoreComputeSnafu);
                }
                #[cfg(feature = "python")]
                FileFormatConfig::PythonFunction => {
                    return Err(common_error::DaftError::TypeError(
                        "Native reads for PythonFunction file format not implemented".to_string(),
                    ))
                    .context(DaftCoreComputeSnafu);
                }
            }
        }
        #[cfg(feature = "python")]
        StorageConfig::Python(_) => match scan_task.file_format_config.as_ref() {
            FileFormatConfig::Parquet(ParquetSourceConfig {
                coerce_int96_timestamp_unit: _,
                ..
            }) => todo!("Implement streaming reads for Parquet with Python Storage Config"),
            FileFormatConfig::Csv(CsvSourceConfig {
                has_headers: _,
                delimiter: _,
                double_quote: _,
                ..
            }) => todo!("Implement streaming reads for CSV with Python Storage Config"),
            FileFormatConfig::Json(_) => {
                todo!("Implement streaming reads for JSON with Python Storage Config")
            }
            FileFormatConfig::Database(daft_scan::file_format::DatabaseSourceConfig {
                sql: _,
                conn: _,
            }) => todo!("Implement streaming reads for Database"),
            FileFormatConfig::PythonFunction => {
                todo!("Implement streaming reads for PythonFunction")
            }
        },
    };

    Ok(table_values)
}

impl MicroPartition {
    /// Create a new "unloaded" MicroPartition using an associated [`ScanTask`]
    ///
    /// Invariants:
    /// 1. Each Loaded column statistic in `statistics` must be castable to the corresponding column in the MicroPartition's schema
    /// 2. Creating a new MicroPartition with a ScanTask that has any filter predicates or limits is not allowed and will panic
    pub fn new_unloaded(
        scan_task: Arc<ScanTask>,
        metadata: TableMetadata,
        statistics: TableStatistics,
    ) -> Self {
        if scan_task.pushdowns.filters.is_some() {
            panic!("Cannot create unloaded MicroPartition from a ScanTask with pushdowns that have filters");
        }

        let schema = scan_task.materialized_schema();
        let fill_map = scan_task.partition_spec().map(|pspec| pspec.to_fill_map());
        let statistics = statistics
            .cast_to_schema_with_fill(schema.clone(), fill_map.as_ref())
            .expect("Statistics cannot be casted to schema");
        MicroPartition {
            schema,
            state: Mutex::new(TableState::Unloaded(scan_task)),
            metadata,
            statistics: Some(statistics),
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
        let schema = scan_task.materialized_schema();
        match (
            &scan_task.metadata,
            &scan_task.statistics,
            scan_task.file_format_config.as_ref(),
            scan_task.storage_config.as_ref(),
        ) {
            // CASE: ScanTask provides all required metadata.
            // If the scan_task provides metadata (e.g. retrieved from a catalog) we can use it to create an unloaded MicroPartition
            (Some(metadata), Some(statistics), _, _) if scan_task.pushdowns.filters.is_none() => {
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
                FileFormatConfig::Parquet(ParquetSourceConfig {
                    coerce_int96_timestamp_unit,
                    field_id_mapping,
                    ..
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
                let parquet_metadata = scan_task
                    .sources
                    .iter()
                    .map(|s| s.get_parquet_metadata().cloned())
                    .collect::<Option<Vec<_>>>();

                let row_groups = parquet_sources_to_row_groups(scan_task.sources.as_slice());

                let mut iceberg_delete_files: HashSet<&str> = HashSet::new();
                for source in scan_task.sources.iter() {
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
                    Some(schema.clone()),
                    field_id_mapping.clone(),
                    parquet_metadata,
                )
                .context(DaftCoreComputeSnafu)
            }

            // CASE: Last resort fallback option
            // Perform an eager **data** read
            _ => {
                let statistics = scan_task.statistics.clone();
                let (tables, schema) = materialize_scan_task(scan_task, Some(io_stats))?;
                Ok(Self::new_loaded(schema, Arc::new(tables), statistics))
            }
        }
    }

    pub async fn from_scan_task_streaming(
        scan_task: Arc<ScanTask>,
        io_stats: IOStatsRef,
        morsel_size: usize,
    ) -> crate::Result<BoxStream<'static, DaftResult<Arc<Self>>>> {
        let schema = scan_task.materialized_schema();
        match (
            &scan_task.metadata,
            &scan_task.statistics,
            scan_task.file_format_config.as_ref(),
            scan_task.storage_config.as_ref(),
        ) {
            // CASE: ScanTask provides all required metadata.
            // If the scan_task provides metadata (e.g. retrieved from a catalog) we can use it to create an unloaded MicroPartition
            (Some(metadata), Some(statistics), _, _) if scan_task.pushdowns.filters.is_none() => {
                let unloaded = Ok(Arc::new(Self::new_unloaded(
                    scan_task.clone(),
                    scan_task
                        .pushdowns
                        .limit
                        .map(|limit| TableMetadata {
                            length: metadata.length.min(limit),
                        })
                        .unwrap_or_else(|| metadata.clone()),
                    statistics.clone(),
                )));
                Ok(Box::pin(futures::stream::iter(std::iter::once(unloaded))))
            }

            // CASE: ScanTask does not provide metadata, but the file format supports metadata retrieval
            // We can perform an eager **metadata** read to create an unloaded MicroPartition
            (
                _,
                _,
                FileFormatConfig::Parquet(ParquetSourceConfig {
                    coerce_int96_timestamp_unit: _,
                    field_id_mapping: _,
                    ..
                }),
                StorageConfig::Native(_cfg),
            ) => {
                todo!()
            }

            // CASE: Last resort fallback option
            // Perform an eager **data** read
            _ => {
                let statistics = scan_task.statistics.clone();
                let stream = stream_scan_task(scan_task.clone(), Some(io_stats)).await?;
                let stream = chunk_tables_into_micropartition_stream(
                    stream,
                    schema,
                    scan_task.partition_spec().cloned(),
                    statistics,
                    morsel_size,
                );
                Ok(Box::pin(stream))
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
        let size_bytes = if let TableState::Loaded(tables) = guard.deref() {
            let total_size: usize = tables
                .iter()
                .map(|t| t.size_bytes())
                .collect::<DaftResult<Vec<_>>>()?
                .iter()
                .sum();
            Some(total_size)
        } else if let TableState::Unloaded(scan_task) = guard.deref() {
            // TODO: pass in the execution config once we have it available
            scan_task.estimate_in_memory_size_bytes(None)
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
                let (tables, _) = materialize_scan_task(scan_task.clone(), Some(io_stats))?;
                let table_values = Arc::new(tables);

                // Cache future accesses by setting the state to TableState::Loaded
                *guard = TableState::Loaded(table_values.clone());

                Ok(table_values)
            }
            TableState::Loaded(tables) => Ok(tables.clone()),
        }
    }

    pub fn concat_or_get(&self, io_stats: IOStatsRef) -> crate::Result<Arc<Vec<Table>>> {
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

        let mut schema_with_id_index_map = self.schema.fields.clone();
        schema_with_id_index_map.insert(
            column_name.to_string(),
            Field::new(column_name, DataType::UInt64),
        );
        let schema_with_id = Schema {
            fields: schema_with_id_index_map,
        };

        Ok(Self::new_loaded(
            Arc::new(schema_with_id),
            Arc::new(tables_with_id),
            self.statistics.clone(),
        ))
    }
}

fn chunk_tables_into_micropartition_stream(
    mut table_stream: BoxStream<'static, DaftResult<Vec<Table>>>,
    schema: SchemaRef,
    partition_spec: Option<PartitionSpec>,
    statistics: Option<TableStatistics>,
    morsel_size: usize,
) -> impl Stream<Item = DaftResult<Arc<MicroPartition>>> + Send {
    async_stream::try_stream! {
        let mut buffer = vec![];
        let mut total_rows = 0;
        while let Some(tables) = table_stream.next().await {
            let tables = tables?;
            for table in tables {
                let casted_table = table.cast_to_schema_with_fill(schema.as_ref(), partition_spec.as_ref().map(|pspec| pspec.to_fill_map()).as_ref())?;
                total_rows += casted_table.len();
                buffer.push(casted_table);
            }
            if total_rows >= morsel_size {
                let mp = Arc::new(MicroPartition::new_loaded(schema.clone(), Arc::new(buffer), statistics.clone()));
                buffer = vec![];
                total_rows = 0;
                yield mp;
            }
        }
        if !buffer.is_empty() {
            let mp = Arc::new(MicroPartition::new_loaded(schema, Arc::new(buffer), statistics));
            yield mp;
        }
    }
}

fn prune_fields_from_schema(
    schema: Arc<Schema>,
    columns: Option<&[&str]>,
) -> DaftResult<Arc<Schema>> {
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
            .as_ref()
            .fields
            .values()
            .filter(|field| names_to_keep.contains(field.name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        Ok(Arc::new(Schema::new(filtered_columns)?))
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

pub(crate) fn read_json_into_micropartition(
    uris: &[&str],
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
    io_config: Arc<IOConfig>,
    multithreaded_io: bool,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<MicroPartition> {
    let io_client = daft_io::get_io_client(multithreaded_io, io_config.clone())?;

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

fn _get_file_column_names<'a>(
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

fn _read_delete_files(
    delete_files: &[&str],
    uris: &[&str],
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    runtime_handle: Arc<tokio::runtime::Runtime>,
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
        runtime_handle,
        schema_infer_options,
        None,
        None,
        None,
    )?;

    let mut delete_map: HashMap<String, Vec<i64>> =
        uris.iter().map(|uri| (uri.to_string(), vec![])).collect();

    for table in tables.iter() {
        // values in the file_path column are guaranteed by the iceberg spec to match the full URI of the corresponding data file
        // https://iceberg.apache.org/spec/#position-delete-files
        let file_paths = table.get_column("file_path")?.downcast::<Utf8Array>()?;
        let positions = table.get_column("pos")?.downcast::<Int64Array>()?;

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
fn _read_parquet_into_loaded_micropartition(
    io_client: Arc<IOClient>,
    runtime_handle: Arc<tokio::runtime::Runtime>,
    uris: &[&str],
    columns: Option<&[&str]>,
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
) -> DaftResult<MicroPartition> {
    let delete_map = iceberg_delete_files
        .map(|files| {
            _read_delete_files(
                files.as_slice(),
                uris,
                io_client.clone(),
                io_stats.clone(),
                num_parallel_tasks,
                runtime_handle.clone(),
                schema_infer_options,
            )
        })
        .transpose()?;

    let file_column_names = _get_file_column_names(columns, partition_spec);
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
        runtime_handle,
        schema_infer_options,
        field_id_mapping,
        None,
        delete_map,
    )?;

    // Prefer using the `catalog_provided_schema` but fall back onto inferred schema from Parquet files
    let full_daft_schema = match catalog_provided_schema {
        Some(catalog_provided_schema) => catalog_provided_schema,
        None => {
            let unioned_schema = all_tables
                .iter()
                .map(|t| t.schema.clone())
                .try_reduce(|l, r| DaftResult::Ok(l.union(&r)?.into()))?;
            unioned_schema.expect("we need at least 1 schema")
        }
    };

    let pruned_daft_schema = prune_fields_from_schema(full_daft_schema, columns)?;

    let fill_map = partition_spec.map(|pspec| pspec.to_fill_map());
    let all_tables = all_tables
        .into_iter()
        .map(|t| t.cast_to_schema_with_fill(&pruned_daft_schema, fill_map.as_ref()))
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
pub(crate) fn read_parquet_into_micropartition(
    uris: &[&str],
    columns: Option<&[&str]>,
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
) -> DaftResult<MicroPartition> {
    if let Some(so) = start_offset
        && so > 0
    {
        return Err(common_error::DaftError::ValueError(
            "Micropartition Parquet Reader does not support non-zero start offsets".to_string(),
        ));
    }

    // Run the required I/O to retrieve all the Parquet FileMetaData
    let runtime_handle = daft_io::get_runtime(multithreaded_io)?;
    let io_client = daft_io::get_io_client(multithreaded_io, io_config.clone())?;

    // If we have a predicate or deletion files then we no longer have an accurate accounting of required metadata
    // on the MicroPartition (e.g. its length). Hence we need to perform an eager read.
    if iceberg_delete_files
        .as_ref()
        .map_or(false, |files| !files.is_empty())
        || predicate.is_some()
    {
        return _read_parquet_into_loaded_micropartition(
            io_client,
            runtime_handle,
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
        );
    }

    // Attempt to read TableStatistics from the Parquet file
    let meta_io_client = io_client.clone();
    let meta_io_stats = io_stats.clone();
    let meta_field_id_mapping = field_id_mapping.clone();
    let (metadata, schemas) = if let Some(metadata) = parquet_metadata {
        let schemas = metadata
            .iter()
            .map(|m| {
                let schema = infer_schema_with_options(m, &Some((*schema_infer_options).into()))?;
                let daft_schema = daft_core::schema::Schema::try_from(&schema)?;
                DaftResult::Ok(Arc::new(daft_schema))
            })
            .collect::<DaftResult<Vec<_>>>()?;
        (metadata, schemas)
    } else {
        let metadata = runtime_handle
            .block_on(async move {
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
                let schema = infer_schema_with_options(m, &Some((*schema_infer_options).into()))?;
                let daft_schema = daft_core::schema::Schema::try_from(&schema)?;
                DaftResult::Ok(Arc::new(daft_schema))
            })
            .collect::<DaftResult<Vec<_>>>()?;
        (metadata, schemas)
    };

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

    // If statistics are provided by the Parquet file, we create an unloaded MicroPartition
    // by constructing an appropriate ScanTask
    if let Some(stats) = stats {
        // Prefer using the `catalog_provided_schema` but fall back onto inferred schema from Parquet files
        let scan_task_daft_schema = match catalog_provided_schema {
            Some(catalog_provided_schema) => catalog_provided_schema,
            None => {
                let unioned_schema = schemas
                    .into_iter()
                    .try_reduce(|l, r| l.union(&r).map(Arc::new))?;
                unioned_schema.expect("we need at least 1 schema")
            }
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
                        .map(|rg_idx| fm.row_groups.get(*rg_idx as usize).unwrap().num_rows())
                        .sum::<usize>(),
                    None => fm.num_rows,
                })
                .sum(),
        };
        let total_rows = num_rows
            .map(|num_rows| num_rows.min(total_rows_no_limit))
            .unwrap_or(total_rows_no_limit);

        let owned_urls = uris.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let size_bytes = metadata
            .iter()
            .map(|m| -> u64 {
                std::iter::Sum::sum(m.row_groups.iter().map(|m| m.total_byte_size() as u64))
            })
            .sum();

        let scan_task = ScanTask::new(
            owned_urls
                .into_iter()
                .zip(metadata)
                .zip(
                    row_groups
                        .clone()
                        .unwrap_or_else(|| std::iter::repeat(None).take(uris.len()).collect()),
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
            })
            .into(),
            scan_task_daft_schema,
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
                None,
                columns
                    .map(|cols| Arc::new(cols.iter().map(|v| v.to_string()).collect::<Vec<_>>())),
                num_rows,
            ),
        );

        let fill_map = scan_task.partition_spec().map(|pspec| pspec.to_fill_map());
        let casted_stats =
            stats.cast_to_schema_with_fill(scan_task.materialized_schema(), fill_map.as_ref())?;

        Ok(MicroPartition::new_unloaded(
            Arc::new(scan_task),
            TableMetadata { length: total_rows },
            casted_stats,
        ))
    } else {
        // If no TableStatistics are available, we perform an eager read
        _read_parquet_into_loaded_micropartition(
            io_client,
            runtime_handle,
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
        )
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
