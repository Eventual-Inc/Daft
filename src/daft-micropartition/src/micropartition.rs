use std::fmt::Display;
use std::sync::Arc;
use std::{ops::Deref, sync::Mutex};

use arrow2::io::parquet::read::schema::infer_schema_with_options;
use common_error::DaftResult;
use daft_core::schema::{Schema, SchemaRef};

use daft_parquet::read::{read_parquet_metadata_bulk, ParquetSchemaInferenceOptions};
use daft_table::Table;

use snafu::ResultExt;

use crate::DaftCoreComputeSnafu;

use crate::table_metadata::TableMetadata;
use crate::table_stats::TableStatistics;
use daft_io::{IOConfig, IOStatsRef};

#[derive(Clone)]
enum FormatParams {
    Parquet(ParquetSchemaInferenceOptions),
}

#[derive(Clone)]
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

    pub(crate) fn tables_or_read(
        &self,
        io_stats: Option<IOStatsRef>,
    ) -> crate::Result<Arc<Vec<Table>>> {
        let mut guard = self.state.lock().unwrap();
        if let TableState::Unloaded(params) = guard.deref() {
            let table_values: Vec<_> = match &params.format_params {
                FormatParams::Parquet(parquet_schema_inference) => {
                    let io_client =
                        daft_io::get_io_client(params.multithreaded_io, params.io_config.clone())
                            .unwrap();
                    let column_names = params
                        .columns
                        .as_ref()
                        .map(|v| v.iter().map(|s| s.as_ref()).collect::<Vec<_>>());
                    let urls = params.urls.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    daft_parquet::read::read_parquet_bulk(
                        urls.as_slice(),
                        column_names.as_deref(),
                        None,
                        params.limit,
                        None,
                        io_client.clone(),
                        io_stats,
                        8,
                        params.multithreaded_io,
                        parquet_schema_inference,
                    )
                    .context(DaftCoreComputeSnafu)?
                }
            };
            *guard = TableState::Loaded(Arc::new(table_values));
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

pub(crate) fn read_parquet_into_micropartition(
    uris: &[&str],
    io_config: Arc<IOConfig>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
) -> DaftResult<MicroPartition> {
    // thread in columns and limit
    let runtime_handle = daft_io::get_runtime(multithreaded_io)?;
    let io_client = daft_io::get_io_client(multithreaded_io, io_config.clone())?;
    let metadata = runtime_handle
        .block_on(async move { read_parquet_metadata_bulk(uris, io_client, io_stats).await })?;

    let vals = metadata
        .iter()
        .flat_map(|fm| fm.row_groups.iter().map(|rg| rg.try_into()))
        .collect::<crate::Result<Vec<TableStatistics>>>()?;

    let folded_stats = vals.into_iter().try_reduce(|a, b| a.union(&b))?;

    let first_metadata = metadata.first().expect("we need at least 1 metadata");
    let schema = infer_schema_with_options(first_metadata, &None)?;

    let daft_schema = daft_core::schema::Schema::try_from(&schema)?;
    let owned_urls = uris.iter().map(|s| s.to_string()).collect::<Vec<_>>();

    let num_rows = metadata.iter().map(|m| m.num_rows).sum();

    let params = DeferredLoadingParams {
        format_params: FormatParams::Parquet(ParquetSchemaInferenceOptions::default()),
        urls: owned_urls,
        io_config: io_config.clone(),
        multithreaded_io: true,
        limit: None,
        columns: None,
    };

    Ok(MicroPartition::new(
        Arc::new(daft_schema),
        TableState::Unloaded(params),
        TableMetadata { length: num_rows },
        folded_stats,
    ))
}

impl Display for MicroPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.state.lock().unwrap();

        writeln!(f, "MicroPartition:")?;

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
