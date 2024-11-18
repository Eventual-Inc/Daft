use std::sync::Arc;

use crate::{
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Distinct {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub stats_state: StatsState,
}

impl Distinct {
    pub(crate) fn new(input: Arc<LogicalPlan>) -> Self {
        Self {
            input,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub(crate) fn materialize_stats(&self) -> Self {
        // TODO(desmond): We can simply use NDVs here. For now, do a naive estimation.
        let new_input = self.input.materialize_stats();
        let input_stats = new_input.get_stats();
        let est_bytes_per_row_lower = input_stats.approx_stats.lower_bound_bytes
            / (input_stats.approx_stats.lower_bound_rows.max(1));
        let approx_stats = ApproxStats {
            lower_bound_rows: input_stats.approx_stats.lower_bound_rows.min(1),
            upper_bound_rows: input_stats.approx_stats.upper_bound_rows,
            lower_bound_bytes: input_stats.approx_stats.lower_bound_bytes.min(1)
                * est_bytes_per_row_lower,
            upper_bound_bytes: input_stats.approx_stats.upper_bound_bytes,
        };
        let stats_state = StatsState::Materialized(PlanStats::new(approx_stats));
        Self {
            input: Arc::new(new_input),
            stats_state,
        }
    }
}
