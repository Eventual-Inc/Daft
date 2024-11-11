use std::sync::Arc;

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Distinct {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Distinct {
    pub(crate) fn new(input: Arc<LogicalPlan>) -> Self {
        Self { input }
    }
}

use crate::stats::{ApproxStats, Stats};
impl Stats for Distinct {
    fn approximate_stats(&self) -> ApproxStats {
        // TODO(desmond): We can simply use NDVs here. For now, do a naive estimation.
        let input_stats = self.input.approximate_stats();
        let est_bytes_per_row_lower =
            input_stats.lower_bound_bytes / (input_stats.lower_bound_rows.max(1));
        ApproxStats {
            lower_bound_rows: input_stats.lower_bound_rows.min(1),
            upper_bound_rows: input_stats.upper_bound_rows,
            lower_bound_bytes: input_stats.lower_bound_bytes.min(1) * est_bytes_per_row_lower,
            upper_bound_bytes: input_stats.upper_bound_bytes,
        }
    }
}
