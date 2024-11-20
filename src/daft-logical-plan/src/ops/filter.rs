use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{ExprRef, ExprResolver};
use snafu::ResultExt;

use crate::{
    logical_plan::{CreationSnafu, Result},
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Filter {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // The Boolean expression to filter on.
    pub predicate: ExprRef,
    pub stats_state: StatsState,
}

impl Filter {
    pub(crate) fn try_new(input: Arc<LogicalPlan>, predicate: ExprRef) -> Result<Self> {
        let expr_resolver = ExprResolver::default();

        let (predicate, field) = expr_resolver
            .resolve_single(predicate, &input.schema())
            .context(CreationSnafu)?;

        if !matches!(field.dtype, DataType::Boolean) {
            return Err(DaftError::ValueError(format!(
                "Expected expression {predicate} to resolve to type Boolean, but received: {}",
                field.dtype
            )))
            .context(CreationSnafu);
        }
        Ok(Self {
            input,
            predicate,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub(crate) fn materialize_stats(&self) -> Self {
        // Assume no row/column pruning in cardinality-affecting operations.
        // TODO(desmond): We can do better estimations here. For now, reuse the old logic.
        let new_input = self.input.materialize_stats();
        let upper_bound_rows = new_input.get_stats().approx_stats.upper_bound_rows;
        let upper_bound_bytes = new_input.get_stats().approx_stats.upper_bound_bytes;
        let approx_stats = ApproxStats {
            lower_bound_rows: 0,
            upper_bound_rows,
            lower_bound_bytes: 0,
            upper_bound_bytes,
        };
        let stats_state = StatsState::Materialized(PlanStats::new(approx_stats));
        Self {
            input: Arc::new(new_input),
            predicate: self.predicate.clone(),
            stats_state,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("Filter: {}", self.predicate)];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
