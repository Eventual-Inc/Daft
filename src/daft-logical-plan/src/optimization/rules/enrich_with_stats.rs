#[derive(Default, Debug)]
pub struct EnrichWithStats {}

impl EnrichWithStats {
    pub fn new() -> Self {
        Self {}
    }
}
use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::Transformed;

use super::OptimizerRule;
use crate::LogicalPlan;

impl OptimizerRule for EnrichWithStats {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        Ok(Transformed::yes(plan.materialize_stats().into()))
    }
}
