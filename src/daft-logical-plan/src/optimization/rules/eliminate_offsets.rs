use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use crate::{ops::Offset as LogicalOffset, optimization::rules::OptimizerRule, LogicalPlan};

/// This rule optimizes Offset operators by:
/// 1. Eliminate `Offset` node if offset == 0
/// 2. Combine two adjacent `Offset` nodes into one by merging their expressions
#[derive(Default, Debug)]
pub struct EliminateOffsets {}

impl EliminateOffsets {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateOffsets {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

impl EliminateOffsets {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            // Eliminate Offset node if offset = 0
            LogicalPlan::Offset(LogicalOffset { offset, input, .. }) if *offset == 0 => {
                Ok(Transformed::yes(input.clone()))
            }
            // Merge adjacent Offset nodes
            LogicalPlan::Offset(LogicalOffset {
                offset: curr_offset,
                input,
                ..
            }) => {
                if let LogicalPlan::Offset(LogicalOffset {
                    offset: child_offset,
                    input: child_input,
                    ..
                }) = input.as_ref()
                {
                    let new_offset = curr_offset + child_offset;
                    let new_node =
                        LogicalPlan::Offset(LogicalOffset::new(child_input.clone(), new_offset));
                    Ok(Transformed::yes(Arc::new(new_node)))
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}
