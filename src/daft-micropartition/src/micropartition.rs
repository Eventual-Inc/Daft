use std::sync::Arc;
use std::{ops::Deref, sync::Mutex};

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;
use daft_core::datatypes::logical::{DateArray, Decimal128Array, TimestampArray};
use daft_core::datatypes::{
    BinaryArray, BooleanArray, DaftNumericType, DaftPhysicalType, DataArray, Int128Array,
    Int32Array, Int64Array, Utf8Array,
};
use daft_core::schema::{Schema, SchemaRef};
use daft_core::{IntoSeries, Series};
use daft_dsl::Expr;
use daft_parquet::read::read_parquet_metadata;
use daft_table::Table;
use indexmap::IndexMap;
use parquet2::statistics::{BinaryStatistics, BooleanStatistics, PrimitiveStatistics, Statistics};
use snafu::ResultExt;

use crate::column_stats::ColumnRangeStatistics;
use crate::DaftCoreComputeSnafu;
use crate::{column_stats::TruthValue, table_stats::TableStatistics};
use daft_io::IOConfig;

struct DeferredLoadingParams {
    filters: Vec<Expr>,
}

enum TableState {
    Unloaded(DeferredLoadingParams),
    Loaded(Vec<Table>),
}

struct MicroPartition {
    schema: SchemaRef,
    state: Mutex<TableState>,
    statistics: Option<TableStatistics>,
}

impl MicroPartition {
    pub fn new(schema: SchemaRef, state: TableState, statistics: Option<TableStatistics>) -> Self {
        MicroPartition {
            schema,
            state: Mutex::new(state),
            statistics: statistics,
        }
    }

    pub fn empty() -> Self {
        Self::new(Schema::empty().into(), TableState::Loaded(vec![]), None)
    }

    fn tables_or_read(&self) -> &[&Table] {
        todo!("to do me")
    }

    pub fn filter(&self, predicate: &[Expr]) -> super::Result<Self> {
        if predicate.is_empty() {
            return Ok(Self::new(
                self.schema.clone(),
                TableState::Loaded(vec![]),
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
                    TableState::Loaded(vec![]),
                    None,
                ));
            }
        }

        let guard = self.state.lock().unwrap();
        let new_state = match guard.deref() {
            TableState::Unloaded(params) => {
                let mut new_filters = params.filters.clone();
                new_filters.extend(predicate.iter().cloned());
                TableState::Unloaded(DeferredLoadingParams {
                    filters: new_filters,
                })
            }
            TableState::Loaded(tables) => TableState::Loaded(
                tables
                    .iter()
                    .map(|t| t.filter(predicate))
                    .collect::<DaftResult<Vec<_>>>()
                    .context(DaftCoreComputeSnafu)?,
            ),
        };

        // TODO: We should also "filter" the TableStatistics so it's more accurate for downstream tasks
        Ok(Self::new(
            self.schema.clone(),
            new_state,
            self.statistics.clone(),
        ))
    }
}

impl From<&daft_parquet::metadata::RowGroupMetaData> for TableStatistics {
    fn from(value: &daft_parquet::metadata::RowGroupMetaData) -> Self {
        let num_rows = value.num_rows();
        let mut columns = IndexMap::new();
        for col in value.columns() {
            let stats = col.statistics().unwrap().unwrap();
            let col_stats: ColumnRangeStatistics = stats.as_ref().into();
            columns.insert(
                col.descriptor().path_in_schema.get(0).unwrap().clone(),
                col_stats,
            );
        }

        TableStatistics { columns }
    }
}

fn read_parquet(uri: &str, io_config: Arc<IOConfig>) -> DaftResult<()> {
    let runtime_handle = daft_io::get_runtime(true)?;
    let io_client = daft_io::get_io_client(true, io_config)?;
    let metadata =
        runtime_handle.block_on(async move { read_parquet_metadata(uri, io_client).await })?;

    for rg in &metadata.row_groups {
        let table_stats: TableStatistics = rg.into();
        println!("{table_stats:?}");
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use common_error::DaftResult;
    use daft_core::{
        array::ops::DaftCompare,
        datatypes::{Int32Array, Int64Array},
        IntoSeries, Series,
    };
    use daft_dsl::{col, lit};
    use daft_io::IOConfig;
    use daft_table::Table;

    use crate::column_stats::TruthValue;

    use super::{ColumnRangeStatistics, TableStatistics};

    #[test]
    fn test_pq() -> crate::Result<()> {
        let url = "/Users/sammy/daft_200MB_lineitem_chunk.RG-2.parquet";
        super::read_parquet(&url, IOConfig::default().into());

        Ok(())
    }
}
