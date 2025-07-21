use std::sync::Arc;

use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use indexmap::IndexMap;
use indicatif::{HumanCount, HumanFloatCount};
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::{
    runtime_stats::{RuntimeStatsBuilder, ROWS_EMITTED_KEY, ROWS_RECEIVED_KEY},
    ExecutionTaskSpawner,
};

pub struct FilterStatsBuilder {}

impl RuntimeStatsBuilder for FilterStatsBuilder {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build(
        &self,
        stats: &mut IndexMap<&'static str, String>,
        rows_received: u64,
        rows_emitted: u64,
    ) {
        stats.insert(ROWS_RECEIVED_KEY, HumanCount(rows_received).to_string());
        stats.insert(ROWS_EMITTED_KEY, HumanCount(rows_emitted).to_string());
        stats.insert(
            "selectivity",
            HumanFloatCount(rows_emitted as f64 / rows_received as f64 * 100.0).to_string(),
        );
    }
}

pub struct FilterOperator {
    predicate: BoundExpr,
}

impl FilterOperator {
    pub fn new(predicate: BoundExpr) -> Self {
        Self { predicate }
    }
}

impl IntermediateOperator for FilterOperator {
    #[instrument(skip_all, name = "FilterOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let predicate = self.predicate.clone();
        task_spawner
            .spawn(
                async move {
                    let out = input.filter(&[predicate])?;
                    Ok((
                        state,
                        IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("Filter: {}", self.predicate)]
    }

    fn name(&self) -> &'static str {
        "Filter"
    }

    fn make_runtime_stats_builder(&self) -> Arc<dyn RuntimeStatsBuilder> {
        Arc::new(FilterStatsBuilder {})
    }
}
