use std::fmt::{write, Display};
use std::sync::Arc;
use std::{ops::Deref, sync::Mutex};

use arrow2::io::parquet::read::schema::infer_schema_with_options;
use common_error::DaftResult;
use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;
use daft_parquet::read::{read_parquet_metadata_bulk, ParquetSchemaInferenceOptions};
use daft_table::Table;

use snafu::ResultExt;

use crate::DaftCoreComputeSnafu;

use crate::{column_stats::TruthValue, table_stats::TableStatistics};
use daft_io::{IOClient, IOConfig, IOStatsRef};

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
    Loaded(Vec<Table>),
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
    pub schema: SchemaRef,
    state: TableState,
    statistics: Option<TableStatistics>,
}

impl MicroPartition {
    pub fn new(schema: SchemaRef, state: TableState, statistics: Option<TableStatistics>) -> Self {
        MicroPartition {
            schema,
            state: state,
            statistics,
        }
    }

    pub fn empty() -> Self {
        Self::new(Schema::empty().into(), TableState::Loaded(vec![]), None)
    }

    fn tables_or_read(&mut self, io_stats: Option<IOStatsRef>) -> crate::Result<&[Table]> {
        if let TableState::Unloaded(params) = &self.state {
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
            self.state = TableState::Loaded(table_values);
        };

        if let TableState::Loaded(tables) = &self.state {
            return Ok(tables.as_slice());
        } else {
            unreachable!()
        }
    }

    pub fn filter(&mut self, predicate: &[Expr]) -> DaftResult<Self> {
        if predicate.is_empty() {
            return Ok(Self::new(
                self.schema.clone(),
                TableState::Loaded(vec![].into()),
                None,
            ));
        }
        if let Some(statistics) = &self.statistics {
            let folded_expr = predicate
                .iter()
                .cloned()
                .reduce(|a, b| a.and(&b))
                .expect("should have at least 1 expr");
            let eval_result = statistics.eval_expression(&folded_expr)?;
            let tv = eval_result.to_truth_value();

            if matches!(tv, TruthValue::False) {
                return Ok(Self::new(
                    self.schema.clone(),
                    TableState::Loaded(vec![].into()),
                    None,
                ));
            }
        }
        // TODO figure out defered IOStats
        let tables = self
            .tables_or_read(None)?
            .iter()
            .map(|t| t.filter(predicate))
            .collect::<DaftResult<Vec<_>>>()
            .context(DaftCoreComputeSnafu)?;

        Ok(Self::new(
            self.schema.clone(),
            TableState::Loaded(tables),
            self.statistics.clone(), // update these values based off the filter we just ran
        ))
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
        folded_stats,
    ))
}

impl Display for MicroPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let guard = self.state.lock().unwrap();
        writeln!(f, "MicroPartition:")?;

        match &self.state {
            TableState::Unloaded(..) => {
                writeln!(f, "{}\n{}", self.schema, self.state)?;
            }
            TableState::Loaded(..) => {
                writeln!(f, "{}", self.state)?;
            }
        };

        match &self.statistics {
            Some(t) => writeln!(f, "Statistics\n{}", t)?,
            None => writeln!(f, "Statistics: missing")?,
        }

        writeln!(f, "Table Data:")?;
        Ok(())
    }
}
#[cfg(test)]
mod test {}
