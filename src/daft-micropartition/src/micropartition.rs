use std::{ops::Deref, sync::Mutex};

use common_error::DaftResult;
use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;
use daft_table::Table;
use snafu::ResultExt;

use crate::DaftCoreComputeSnafu;
use crate::{column_stats::TruthValue, table_stats::TableStatistics};

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
        Ok(Self::new(
            self.schema.clone(),
            new_state,
            self.statistics.clone(),
        ))
    }
}
