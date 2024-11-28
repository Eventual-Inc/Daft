#[derive(Default, Debug)]
pub struct EnrichWithStats {}

impl EnrichWithStats {
    pub fn new() -> Self {
        Self {}
    }
}
use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use super::OptimizerRule;
use crate::LogicalPlan;

// Add stats to all logical plan nodes in a bottom up fashion.
// All scan nodes MUST be materialized before stats are enriched.
impl OptimizerRule for EnrichWithStats {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(|node: Arc<LogicalPlan>| {
            Ok(Transformed::yes(
                Arc::unwrap_or_clone(node).with_materialized_stats().into(),
            ))
        })
    }
}
