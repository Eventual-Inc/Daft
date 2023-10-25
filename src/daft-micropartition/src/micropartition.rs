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
use daft_table::Table;

use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::DaftCoreComputeSnafu;

use daft_io::{IOConfig, IOStatsRef};
use daft_stats::TableMetadata;
use daft_stats::TableStatistics;

#[derive(Clone, Serialize, Deserialize)]
enum FormatParams {
    Parquet {
        row_groups: Option<Vec<Vec<i64>>>,
        inference_options: ParquetSchemaInferenceOptions,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct DeferredLoadingParams {
    format_params: FormatParams,
    urls: Vec<String>,
    io_config: Arc<IOConfig>,
    multithreaded_io: bool,
    limit: Option<usize>,
    columns: Option<Vec<String>>,
}
pub(crate) enum TableState {
    Unloaded(DeferredLoadingParams),
    Loaded(Arc<Vec<Table>>),
}

impl Display for TableState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableState::Unloaded(params) => {
                write!(f, "TableState: Unloaded. To load from: {:?}", params.urls)
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

impl MicroPartition {
    pub fn new(
        schema: SchemaRef,
        state: TableState,
        metadata: TableMetadata,
        statistics: Option<TableStatistics>,
    ) -> Self {
        if let TableState::Unloaded(..) = state && statistics.is_none() {
            panic!("MicroPartition does not allow the Table without Statistics")
        }
        if let Some(stats) = &statistics {
            if stats.columns.len() != schema.fields.len() {
                panic!("MicroPartition: TableStatistics and Schema have differing lengths")
            }
            if !stats
                .columns
                .keys()
                .zip(schema.fields.keys())
                .all(|(l, r)| l == r)
            {
                panic!("MicroPartition: TableStatistics and Schema have different column names\nTableStats:\n{},\nSchema\n{}", stats, schema);
            }
        }

        MicroPartition {
            schema,
            state: Mutex::new(state),
            metadata,
            statistics,
        }
    }

    pub fn empty(schema: Option<SchemaRef>) -> Self {
        let schema = schema.unwrap_or(Schema::empty().into());

        Self::new(
            schema,
            TableState::Loaded(Arc::new(vec![])),
            TableMetadata { length: 0 },
            None,
        )
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
        if let TableState::Unloaded(params) = guard.deref() {
            let runtime_handle = daft_io::get_runtime(params.multithreaded_io).unwrap();
            let _rt_guard = runtime_handle.enter();

            let table_values: Vec<_> = match &params.format_params {
                FormatParams::Parquet {
                    row_groups,
                    inference_options,
                } => {
                    let io_client =
                        daft_io::get_io_client(params.multithreaded_io, params.io_config.clone())
                            .unwrap();
                    let column_names = params
                        .columns
                        .as_ref()
                        .map(|v| v.iter().map(|s| s.as_ref()).collect::<Vec<_>>());
                    let urls = params.urls.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    let all_tables = daft_parquet::read::read_parquet_bulk(
                        urls.as_slice(),
                        column_names.as_deref(),
                        None,
                        params.limit,
                        row_groups.clone(),
                        io_client.clone(),
                        io_stats,
                        8,
                        runtime_handle,
                        inference_options,
                    )
                    .context(DaftCoreComputeSnafu)?;
                    all_tables
                        .into_iter()
                        .map(|t| t.cast_to_schema(&self.schema))
                        .collect::<DaftResult<Vec<_>>>()
                        .context(DaftCoreComputeSnafu)?
                }
            };
            let casted_table_values = table_values
                .iter()
                .map(|tbl| tbl.cast_to_schema(self.schema.as_ref()))
                .collect::<DaftResult<Vec<_>>>()
                .context(DaftCoreComputeSnafu)?;
            *guard = TableState::Loaded(Arc::new(casted_table_values));
        };

        if let TableState::Loaded(tables) = guard.deref() {
            Ok(tables.clone())
        } else {
            unreachable!()
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
            let total_len = tables.iter().map(|t| t.len()).sum();
            Ok(MicroPartition::new(
                unioned_schema.clone(),
                TableState::Loaded(Arc::new(tables)),
                TableMetadata { length: total_len },
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
        let owned_columns = columns.map(|c| c.iter().map(|s| s.to_string()).collect::<Vec<_>>());

        let params = DeferredLoadingParams {
            format_params: FormatParams::Parquet {
                row_groups,
                inference_options: *schema_infer_options,
            },
            urls: owned_urls,
            io_config: io_config.clone(),
            multithreaded_io,
            limit: num_rows,
            columns: owned_columns,
        };

        let exprs = daft_schema
            .fields
            .keys()
            .map(|n| daft_dsl::col(n.as_str()))
            .collect::<Vec<_>>();
        // use schema to update stats
        let stats = stats.eval_expression_list(exprs.as_slice(), &daft_schema)?;

        Ok(MicroPartition::new(
            Arc::new(daft_schema),
            TableState::Unloaded(params),
            TableMetadata { length: total_rows },
            Some(stats),
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
        Ok(MicroPartition::new(
            Arc::new(daft_schema),
            TableState::Loaded(all_tables.into()),
            TableMetadata { length: total_rows },
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
