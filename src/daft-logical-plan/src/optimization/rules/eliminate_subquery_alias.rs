use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use super::OptimizerRule;
use crate::{logical_plan::SubqueryAlias, LogicalPlan};

/// Optimization rule for eliminating subquery aliases.
///
/// Aliases are used to resolve columns,
/// but once a logical plan is fully constructed, they are no longer needed
/// and add complexity to things like optimizer rules if kept.
///
/// This rule simply removes subquery aliases so that the plan becomes simpler.
#[derive(Default, Debug)]
pub struct EliminateSubqueryAliasRule {}

impl EliminateSubqueryAliasRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateSubqueryAliasRule {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(|p| {
            if let LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) = p.as_ref() {
                Ok(Transformed::yes(input.clone()))
            } else {
                Ok(Transformed::no(p))
            }
        })
    }
}
