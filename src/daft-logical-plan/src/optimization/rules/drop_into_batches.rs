use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{DynTreeNode, Transformed, TreeNode};

use super::OptimizerRule;
use crate::LogicalPlan;

/// Optimization rule for dropping unnecessary IntoBatches.
///
/// Dropping of upstream IntoBatches for back-to-back IntoBatches happens during
/// logical -> physical plan translation.
#[derive(Default, Debug)]
pub struct DropIntoBatches {}

impl DropIntoBatches {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for DropIntoBatches {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| {
            let into_batches = match node.as_ref() {
                LogicalPlan::IntoBatches(into_batches) => into_batches,
                _ => return Ok(Transformed::no(node)),
            };
            let child_plan = into_batches.input.as_ref();
            let new_plan = match child_plan {
                LogicalPlan::IntoBatches(_) => {
                    // Drop upstream IntoBatches for back-to-back IntoBatches.
                    //
                    // IntoBatches1-IntoBatches2 -> IntoBatches1
                    node.with_new_children(&[child_plan.arc_children()[0].clone()])
                        .into()
                }
                _ => return Ok(Transformed::no(node)),
            };
            Ok(Transformed::yes(new_plan))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::*;

    use crate::{
        LogicalPlan,
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::drop_into_batches::DropIntoBatches,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
    };

    /// Helper that creates an optimizer with the DropIntoBatches rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(DropIntoBatches::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    /// Tests that DropIntoBatches drops the upstream IntoBatches in back-to-back IntoBatches.
    ///
    /// IntoBatches1-IntoBatches2 -> IntoBatches1
    #[test]
    fn into_batches_dropped_in_back_to_back() -> DaftResult<()> {
        let batch_size_1 = 10;
        let batch_size_2 = 5;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .into_batches(batch_size_1)?
            .into_batches(batch_size_2)?
            .build();
        let expected = dummy_scan_node(scan_op).into_batches(batch_size_2)?.build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
