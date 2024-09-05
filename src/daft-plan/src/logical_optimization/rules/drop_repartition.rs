use std::sync::Arc;

use common_error::DaftResult;

use crate::{optimizer::OptimizerRule, LogicalPlan};

use common_treenode::{Transformed, TreeNode};

/// Optimization rules for dropping unnecessary Repartitions.
///
/// Dropping of Repartitions that would yield the same partitioning as their input
/// happens during logical -> physical plan translation.
#[derive(Debug)]
pub struct DropRepartition {}

impl DropRepartition {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule<LogicalPlan> for DropRepartition {
    fn apply(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(|p| {
            if let LogicalPlan::Repartition(repart) = p.as_ref()
                && let LogicalPlan::Repartition(input_repart) = repart.input.as_ref()
            {
                // Drop upstream Repartition for back-to-back Repartitions.
                //
                // Repartition1-Repartition2 -> Repartition1
                Ok(Transformed::yes(
                    p.with_new_children(&[input_repart.input.clone()]).into(),
                ))
            } else {
                Ok(Transformed::no(p))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::col;
    use std::sync::Arc;

    use crate::{
        assert_optimized_plan_with_batches_eq,
        logical_optimization::rules::DropRepartition,
        optimizer::{RuleBatch, RuleBatchStrategy},
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
        assert_optimized_plan_with_batches_eq!(
            LogicalPlan,
            plan,
            expected,
            vec![RuleBatch::new(
                "Drop Repartition",
                vec![Box::new(DropRepartition::new())],
                RuleBatchStrategy::Once,
            )]
        );

        Ok(())
    }

    /// Tests that DropRepartition does drops the upstream Repartition in back-to-back Repartitions.
    ///
    /// Repartition1-Repartition2 -> Repartition1
    #[test]
    fn repartition_dropped_in_back_to_back() -> DaftResult<()> {
        let num_partitions1 = 10;
        let num_partitions2 = 5;
        let partition_by = vec![col("a")];
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .hash_repartition(Some(num_partitions1), partition_by.clone())?
            .hash_repartition(Some(num_partitions2), partition_by.clone())?
            .build();
        let expected = dummy_scan_node(scan_op)
            .hash_repartition(Some(num_partitions2), partition_by.clone())?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that DropRepartition does drops the upstream Repartition in back-to-back-to-back Repartitions.
    ///
    /// Repartition1-Repartition2-Repartition3 -> Repartition1
    #[test]
    fn repartition_dropped_in_back_to_back_to_back() -> DaftResult<()> {
        let num_partitions1 = 10;
        let num_partitions2 = 5;
        let num_partitions3 = 20;
        let partition_by = vec![col("a")];
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .hash_repartition(Some(num_partitions1), partition_by.clone())?
            .hash_repartition(Some(num_partitions2), partition_by.clone())?
            .hash_repartition(Some(num_partitions3), partition_by.clone())?
            .build();
        let expected = dummy_scan_node(scan_op)
            .hash_repartition(Some(num_partitions3), partition_by.clone())?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
