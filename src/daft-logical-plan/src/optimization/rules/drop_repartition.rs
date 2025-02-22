use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{DynTreeNode, Transformed, TreeNode};

use super::OptimizerRule;
use crate::LogicalPlan;

/// Optimization rules for dropping unnecessary Repartitions.
///
/// Dropping of Repartitions that would yield the same partitioning as their input
/// happens during logical -> physical plan translation.
#[derive(Default, Debug)]
pub struct DropRepartition {}

impl DropRepartition {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for DropRepartition {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| {
            let repartition = match node.as_ref() {
                LogicalPlan::Repartition(repartition) => repartition,
                _ => return Ok(Transformed::no(node)),
            };
            let child_plan = repartition.input.as_ref();
            let new_plan = match child_plan {
                LogicalPlan::Repartition(_) => {
                    // Drop upstream Repartition for back-to-back Repartitions.
                    //
                    // Repartition1-Repartition2 -> Repartition1
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
    use daft_dsl::unresolved_col;

    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::drop_repartition::DropRepartition,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
    };

    /// Helper that creates an optimizer with the DropRepartition rule registered, optimizes
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
                vec![Box::new(DropRepartition::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    /// Tests that DropRepartition does drops the upstream Repartition in back-to-back Repartitions.
    ///
    /// Repartition1-Repartition2 -> Repartition1
    #[test]
    fn repartition_dropped_in_back_to_back() -> DaftResult<()> {
        let num_partitions1 = 10;
        let num_partitions2 = 5;
        let partition_by = vec![unresolved_col("a")];
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .hash_repartition(Some(num_partitions1), partition_by.clone())?
            .hash_repartition(Some(num_partitions2), partition_by.clone())?
            .build();
        let expected = dummy_scan_node(scan_op)
            .hash_repartition(Some(num_partitions2), partition_by)?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
