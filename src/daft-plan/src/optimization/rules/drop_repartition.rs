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
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::drop_repartition::DropRepartition,
            Optimizer,
        },
        test::dummy_scan_node,
        LogicalPlan, PartitionScheme,
    };

    /// Helper that creates an optimizer with the DropRepartition rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan's repr with
    /// the provided expected repr.
    fn assert_optimized_plan_eq(plan: Arc<LogicalPlan>, expected: &str) -> DaftResult<()> {
        let optimizer = Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![Box::new(DropRepartition::new())],
                RuleExecutionStrategy::Once,
            )],
            Default::default(),
        );
        let optimized_plan = optimizer
            .optimize_with_rules(
                optimizer.rule_batches[0].rules.as_slice(),
                plan.clone(),
                &optimizer.rule_batches[0].order,
            )?
            .unwrap()
            .clone();
        assert_eq!(optimized_plan.repr_indent(), expected);

        Ok(())
    }

    /// Tests that DropRepartition does drops the upstream Repartition in back-to-back Repartitions if .
    ///
    /// Repartition1-Repartition2 -> Repartition1
    #[test]
    fn repartition_dropped_in_back_to_back() -> DaftResult<()> {
        let plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .repartition(Some(10), vec![col("a")], PartitionScheme::Hash)?
        .repartition(Some(5), vec![col("a")], PartitionScheme::Hash)?
        .build();
        let expected = "\
        Repartition: Scheme = Hash, Number of partitions = 5, Partition by = col(a)\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None, multithreaded_io: true }), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
