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
use crate::{stats::StatsState, LogicalPlan};

// Add stats to all logical plan nodes in a bottom up fashion.
// All scan nodes MUST be materialized before stats are enriched.
impl OptimizerRule for EnrichWithStats {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(|node: Arc<LogicalPlan>| {
            let node = Arc::unwrap_or_clone(node);
            if matches!(node.stats_state(), StatsState::Materialized(_)) {
                Ok(Transformed::no(node.arced()))
            } else {
                Ok(Transformed::yes(node.with_materialized_stats().into()))
            }
        })
    }
}
