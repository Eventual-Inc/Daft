use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use std::{ops::Deref, sync::Mutex};

use arrow2::io::parquet::read::schema::infer_schema_with_options;
use common_error::DaftResult;
use daft_core::schema::{Schema, SchemaRef};

use daft_csv::read::read_csv;
use daft_parquet::read::{
    read_parquet_bulk, read_parquet_metadata_bulk, ParquetSchemaInferenceOptions,
};
use daft_scan::file_format::{FileFormatConfig, ParquetSourceConfig};
use daft_scan::storage_config::{NativeStorageConfig, StorageConfig};
use daft_scan::{DataFileSource, ScanTask};
use daft_table::Table;

use snafu::ResultExt;

use crate::DaftCoreComputeSnafu;

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
) -> crate::Result<Vec<Table>> {
    let table_values = match scan_task.file_format_config.as_ref() {
        FileFormatConfig::Parquet(ParquetSourceConfig {
            coerce_int96_timestamp_unit,
            // TODO(Clark): Support different row group specification per file.
            row_groups,
        }) => match scan_task.storage_config.as_ref() {
            StorageConfig::Native(native_storage_config) => {
                let runtime_handle =
                    daft_io::get_runtime(native_storage_config.multithreaded_io).unwrap();
                let _rt_guard = runtime_handle.enter();

                let io_config = Arc::new(
                    native_storage_config
                        .io_config
                        .as_ref()
                        .cloned()
                        .unwrap_or_default(),
                );
                let io_client =
                    daft_io::get_io_client(native_storage_config.multithreaded_io, io_config)
                        .unwrap();
                let column_names = scan_task
                    .columns
                    .as_ref()
                    .map(|v| v.iter().map(|s| s.as_ref()).collect::<Vec<_>>());
                let urls = scan_task
                    .sources
                    .iter()
                    .map(|s| s.get_path())
                    .collect::<Vec<_>>();
                let inference_options =
                    ParquetSchemaInferenceOptions::new(Some(*coerce_int96_timestamp_unit));
                daft_parquet::read::read_parquet_bulk(
                    urls.as_slice(),
                    column_names.as_deref(),
                    None,
                    scan_task.limit,
                    row_groups
                        .as_ref()
                        .map(|row_groups| vec![row_groups.clone(); urls.len()]),
                    io_client.clone(),
                    io_stats,
                    8,
                    runtime_handle,
                    &inference_options,
                )
                .context(DaftCoreComputeSnafu)?
            }
            #[cfg(feature = "python")]
            StorageConfig::Python(_) => {
                todo!("TODO: Implement Python I/O backend for MicroPartitions.")
            }
        },
        _ => todo!("TODO: Implement MicroPartition reads for other file formats."),
    };

    let cast_to_schema = cast_to_schema.unwrap_or_else(|| scan_task.schema.clone());
    let casted_table_values = table_values
        .iter()
        .map(|tbl| tbl.cast_to_schema(cast_to_schema.as_ref()))
        .collect::<DaftResult<Vec<_>>>()
        .context(DaftCoreComputeSnafu)?;
    Ok(casted_table_values)
}

impl MicroPartition {
    pub fn new_unloaded(
        schema: SchemaRef,
        scan_task: Arc<ScanTask>,
        metadata: TableMetadata,
        statistics: TableStatistics,
    ) -> Self {
        if statistics.columns.len() != schema.fields.len() {
            panic!("MicroPartition: TableStatistics and Schema have differing lengths")
        }
        if !statistics
            .columns
            .keys()
            .zip(schema.fields.keys())
            .all(|(l, r)| l == r)
        {
            panic!("MicroPartition: TableStatistics and Schema have different column names\nTableStats:\n{},\nSchema\n{}", statistics, schema);
        }

        MicroPartition {
            schema,
            state: Mutex::new(TableState::Unloaded(scan_task)),
            metadata,
            statistics: Some(statistics),
        }
    }

    pub fn new_loaded(
        schema: SchemaRef,
        tables: Arc<Vec<Table>>,
        statistics: Option<TableStatistics>,
    ) -> Self {
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

    pub fn from_scan_task(
        scan_task: Arc<ScanTask>,
        io_stats: Option<IOStatsRef>,
    ) -> crate::Result<Self> {
        let schema = scan_task.schema.clone();
        let statistics = scan_task.statistics.clone();
        match (&scan_task.metadata, &scan_task.statistics) {
            (Some(metadata), Some(statistics)) => Ok(Self::new_unloaded(
                schema,
                scan_task.clone(),
                metadata.clone(),
                statistics.clone(),
            )),
            // If any metadata or statistics are missing, we need to perform an eager read
            _ => {
                let tables = materialize_scan_task(scan_task, None, io_stats)?;
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

    pub fn size_bytes(&self) -> DaftResult<usize> {
        {
            let guard = self.state.lock().unwrap();
            if let TableState::Loaded(tables) = guard.deref() {
                let total_size: usize = tables
                    .iter()
                    .map(|t| t.size_bytes())
                    .collect::<DaftResult<Vec<_>>>()?
                    .iter()
                    .sum();
                return Ok(total_size);
            }
        }
        if let Some(stats) = &self.statistics {
            let row_size = stats.estimate_row_size()?;
            Ok(row_size * self.len())
        } else {
            // if the table is not loaded and we dont have stats, just return 0.
            // not sure if we should pull the table in for this
            Ok(0)
        }
    }

    pub(crate) fn tables_or_read(
        &self,
        io_stats: Option<IOStatsRef>,
    ) -> crate::Result<Arc<Vec<Table>>> {
        let mut guard = self.state.lock().unwrap();
        match guard.deref() {
            TableState::Unloaded(scan_task) => {
                let table_values = Arc::new(materialize_scan_task(
                    scan_task.clone(),
                    Some(self.schema.clone()),
                    io_stats,
                )?);

                // Cache future accesses by setting the state to TableState::Loaded
                *guard = TableState::Loaded(table_values.clone());

                Ok(table_values)
            }
            TableState::Loaded(tables) => Ok(tables.clone()),
        }
    }

    pub(crate) fn concat_or_get(&self) -> crate::Result<Arc<Vec<Table>>> {
        let tables = self.tables_or_read(None)?;
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn read_csv_into_micropartition(
    uris: &[&str],
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: Option<u8>,
    double_quote: bool,
    io_config: Arc<IOConfig>,
    multithreaded_io: bool,
    io_stats: Option<IOStatsRef>,
    schema: Option<SchemaRef>,
    buffer_size: Option<usize>,
    chunk_size: Option<usize>,
) -> DaftResult<MicroPartition> {
    let io_client = daft_io::get_io_client(multithreaded_io, io_config.clone())?;
    let mut remaining_rows = num_rows;

    match uris {
        [] => Ok(MicroPartition::empty(None)),
        uris => {
            // Naively load CSVs from URIs
            let mut tables = vec![];
            for uri in uris {
                // Terminate early if we have read enough rows already
                if remaining_rows.map(|rr| rr == 0).unwrap_or(false) {
                    break;
                }
                let table = read_csv(
                    uri,
                    column_names.clone(),
                    include_columns.clone(),
                    remaining_rows,
                    has_header,
                    delimiter,
                    double_quote,
                    io_client.clone(),
                    io_stats.clone(),
                    multithreaded_io,
                    schema.clone(),
                    buffer_size,
                    chunk_size,
                    None,
                )?;
                remaining_rows = remaining_rows.map(|rr| rr - table.len());
                tables.push(table);
            }

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
    row_groups: Option<Vec<Vec<i64>>>,
    io_config: Arc<IOConfig>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
    multithreaded_io: bool,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<MicroPartition> {
    if let Some(so) = start_offset && so > 0 {
        return Err(common_error::DaftError::ValueError("Micropartition Parquet Reader does not support non-zero start offsets".to_string()));
    }

    let runtime_handle = daft_io::get_runtime(multithreaded_io)?;
    let io_client = daft_io::get_io_client(multithreaded_io, io_config.clone())?;

    let meta_io_client = io_client.clone();
    let meta_io_stats = io_stats.clone();

    let metadata = runtime_handle.block_on(async move {
        read_parquet_metadata_bulk(uris, meta_io_client, meta_io_stats).await
    })?;
    let any_stats_avail = metadata
        .iter()
        .flat_map(|m| m.row_groups.iter())
        .flat_map(|rg| rg.columns().iter())
        .any(|col| col.statistics().is_some());
    let stats = if any_stats_avail {
        let stat_per_table = metadata
            .iter()
            .flat_map(|fm| {
                fm.row_groups
                    .iter()
                    .map(daft_parquet::row_group_metadata_to_table_stats)
            })
            .collect::<DaftResult<Vec<TableStatistics>>>()?;
        stat_per_table.into_iter().try_reduce(|a, b| a.union(&b))?
    } else {
        None
    };

    let schemas = metadata
        .iter()
        .map(|m| {
            let schema = infer_schema_with_options(m, &Some((*schema_infer_options).into()))?;
            let daft_schema = daft_core::schema::Schema::try_from(&schema)?;
            DaftResult::Ok(daft_schema)
        })
        .collect::<DaftResult<Vec<_>>>()?;

    let unioned_schema = schemas.into_iter().try_reduce(|l, r| l.union(&r))?;

    let daft_schema = unioned_schema.expect("we need at least 1 schema");

    let daft_schema = prune_fields_from_schema(daft_schema, columns)?;

    // Get total number of rows, accounting for selected `row_groups` and the indicated `num_rows`
    let total_rows_no_limit = match &row_groups {
        None => metadata.iter().map(|fm| fm.num_rows).sum(),
        Some(row_groups) => metadata
            .iter()
            .zip(row_groups.iter())
            .map(|(fm, rg)| {
                rg.iter()
                    .map(|rg_idx| fm.row_groups.get(*rg_idx as usize).unwrap().num_rows())
                    .sum::<usize>()
            })
            .sum(),
    };
    let total_rows = num_rows
        .map(|num_rows| num_rows.min(total_rows_no_limit))
        .unwrap_or(total_rows_no_limit);

    if let Some(stats) = stats {
        let owned_urls = uris.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        let daft_schema = Arc::new(daft_schema);
        let scan_task = ScanTask::new(
            owned_urls
                .into_iter()
                .map(|url| DataFileSource::AnonymousDataFile {
                    path: url,
                    metadata: None,
                    partition_spec: None,
                    statistics: None,
                })
                .collect::<Vec<_>>(),
            FileFormatConfig::Parquet(ParquetSourceConfig {
                coerce_int96_timestamp_unit: schema_infer_options.coerce_int96_timestamp_unit,
                row_groups: None,
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
            columns.map(|cols| Arc::new(cols.iter().map(|v| v.to_string()).collect::<Vec<_>>())),
            num_rows,
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
            .map(|t| t.cast_to_schema(&daft_schema))
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(MicroPartition::new_loaded(
            Arc::new(daft_schema),
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
