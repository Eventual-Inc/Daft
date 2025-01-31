use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{estimated_selectivity, ExprRef};
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
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
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        predicate: ExprRef,
    ) -> logical_plan::Result<Self> {
        let dtype = predicate.to_field(&input.schema())?.dtype;

        if !matches!(dtype, DataType::Boolean) {
            return Err(DaftError::ValueError(format!(
                "Expected expression {predicate} to resolve to type Boolean, but received: {}",
                dtype
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
        let estimated_selectivity = estimated_selectivity(&self.predicate, &self.input.schema());
        let approx_stats = ApproxStats {
            num_rows: (input_stats.approx_stats.num_rows as f64 * estimated_selectivity).ceil()
                as usize,
            size_bytes: (input_stats.approx_stats.size_bytes as f64 * estimated_selectivity).ceil()
                as usize,
            acc_selectivity: input_stats.approx_stats.acc_selectivity * estimated_selectivity,
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
