use std::sync::Arc;

use common_error::DaftResult;

use crate::LogicalPlan;

pub enum ApplyOrder {
    TopDown,
    #[allow(dead_code)]
    BottomUp,
}

// TODO(Clark): Add fixed-point policy if needed.
pub trait OptimizerRule {
    /// Try to optimize the logical plan with this rule.
    ///
    /// This returns Some(new_plan) if the rule modified the plan, None otherwise.
    fn try_optimize(&self, plan: &LogicalPlan) -> DaftResult<Option<Arc<LogicalPlan>>>;

    /// The plan tree order in which this rule should be applied (top-down or bottom-up).
    fn apply_order(&self) -> Option<ApplyOrder>;
}
