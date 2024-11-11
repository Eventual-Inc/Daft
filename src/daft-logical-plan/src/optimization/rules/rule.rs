use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::Transformed;

use crate::LogicalPlan;

/// A logical plan optimization rule.
pub trait OptimizerRule {
    /// Try to optimize the logical plan with this rule.
    ///
    /// This returns Transformed::yes(new_plan) if the rule modified the plan, Transformed::no(old_plan) otherwise.
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>>;
}
