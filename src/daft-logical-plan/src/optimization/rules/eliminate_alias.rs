use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use super::OptimizerRule;
use crate::{logical_plan::Alias, LogicalPlan};

/// Optimization rule for eliminating plan aliases.
///
/// Aliases are used to resolve unresolved columns,
/// but once a logical plan is fully constructed, they are no longer needed
/// and add complexity to things like optimizer rules if kept.
///
/// This rule simply removes plan aliases so that the plan becomes simpler.
#[derive(Default, Debug)]
pub struct EliminateAliasRule {}

impl EliminateAliasRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateAliasRule {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(|p| {
            if let LogicalPlan::Alias(Alias { input, .. }) = p.as_ref() {
                Ok(Transformed::yes(input.clone()))
            } else {
                Ok(Transformed::no(p))
            }
        })
    }
}
