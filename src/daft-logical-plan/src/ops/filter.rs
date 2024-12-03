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

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // Assume no row/column pruning in cardinality-affecting operations.
        // TODO(desmond): We can do better estimations here. For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        let upper_bound_rows = input_stats.approx_stats.upper_bound_rows;
        let upper_bound_bytes = input_stats.approx_stats.upper_bound_bytes;
        let approx_stats = ApproxStats {
            lower_bound_rows: 0,
            upper_bound_rows,
            lower_bound_bytes: 0,
            upper_bound_bytes,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("Filter: {}", self.predicate)];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
