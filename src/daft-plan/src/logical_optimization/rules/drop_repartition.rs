use std::sync::Arc;

use common_error::DaftResult;

use crate::LogicalPlan;

use super::{ApplyOrder, OptimizerRule, Transformed};

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
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::TopDown
    }

    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let repartition = match plan.as_ref() {
            LogicalPlan::Repartition(repartition) => repartition,
            _ => return Ok(Transformed::No(plan)),
        };
        let child_plan = repartition.input.as_ref();
        let new_plan = match child_plan {
            LogicalPlan::Repartition(_) => {
                // Drop upstream Repartition for back-to-back Repartitions.
                //
                // Repartition1-Repartition2 -> Repartition1
                plan.with_new_children(&[child_plan.children()[0].clone()])
                    .into()
            }
            _ => return Ok(Transformed::No(plan)),
        };
        Ok(Transformed::Yes(new_plan))
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::col;
    use std::sync::Arc;

    use crate::{
        logical_optimization::{
            rules::drop_repartition::DropRepartition, test::assert_optimized_plan_with_rules_eq,
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
        assert_optimized_plan_with_rules_eq(plan, expected, vec![Box::new(DropRepartition::new())])
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
}
