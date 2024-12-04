use common_error::DaftResult;

use super::rules::{
    drop_repartition::DropRepartitionPhysical, reorder_partition_keys::ReorderPartitionKeys,
    PhysicalOptimizerRuleBatch, PhysicalRuleExecutionStrategy,
};
use crate::PhysicalPlanRef;

pub struct PhysicalOptimizerConfig {
    // The upper bound on the number of passes a rule batch can run.
    // Depending on its configuration a rule batch may run fewer passes.
    // Default is 5
    pub max_passes: usize,
}

impl PhysicalOptimizerConfig {
    #[allow(dead_code)] // used in test
    pub fn new(max_passes: usize) -> Self {
        Self { max_passes }
    }
}

impl Default for PhysicalOptimizerConfig {
    fn default() -> Self {
        Self { max_passes: 5 }
    }
}

pub struct PhysicalOptimizer {
    rule_batches: Vec<PhysicalOptimizerRuleBatch>,
    config: PhysicalOptimizerConfig,
}

impl PhysicalOptimizer {
    #[allow(dead_code)] // used in test
    pub fn new(
        rule_batches: Vec<PhysicalOptimizerRuleBatch>,
        config: PhysicalOptimizerConfig,
    ) -> Self {
        Self {
            rule_batches,
            config,
        }
    }

    pub fn optimize(&self, mut plan: PhysicalPlanRef) -> DaftResult<PhysicalPlanRef> {
        for batch in &self.rule_batches {
            plan = batch.optimize(plan, &self.config)?;
        }
        Ok(plan)
    }
}

impl Default for PhysicalOptimizer {
    fn default() -> Self {
        Self {
            rule_batches: vec![PhysicalOptimizerRuleBatch::new(
                vec![
                    Box::new(ReorderPartitionKeys {}),
                    Box::new(DropRepartitionPhysical {}),
                ],
                PhysicalRuleExecutionStrategy::Once,
            )],
            config: PhysicalOptimizerConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{assert_matches::assert_matches, sync::Arc};

    use common_error::DaftResult;
    use common_treenode::Transformed;
    use daft_core::prelude::*;
    use daft_logical_plan::partitioning::{ClusteringSpec, UnknownClusteringConfig};

    use super::{PhysicalOptimizer, PhysicalOptimizerRuleBatch};
    use crate::{
        ops::{EmptyScan, Limit},
        optimization::{optimizer::PhysicalOptimizerConfig, rules::PhysicalOptimizerRule},
        PhysicalPlan, PhysicalPlanRef,
    };

    fn create_dummy_plan(schema: SchemaRef, num_partitions: usize) -> PhysicalPlanRef {
        PhysicalPlan::EmptyScan(EmptyScan::new(
            schema,
            ClusteringSpec::Unknown(UnknownClusteringConfig::new(num_partitions)).into(),
        ))
        .into()
    }

    // rule that increments Limit's limit by 1 per pass
    struct CountingRule {
        pub cutoff: i64,
    }

    impl PhysicalOptimizerRule for CountingRule {
        fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>> {
            match plan.as_ref() {
                PhysicalPlan::Limit(Limit {
                    input,
                    limit,
                    eager,
                    num_partitions,
                }) => {
                    if *limit >= self.cutoff {
                        Ok(Transformed::no(plan))
                    } else {
                        let new_plan = PhysicalPlan::Limit(Limit::new(
                            input.clone(),
                            limit + 1,
                            *eager,
                            *num_partitions,
                        ));
                        Ok(Transformed::yes(new_plan.arced()))
                    }
                }
                _ => panic!("expected Limit"),
            }
        }
    }

    // test that Once rule batches only execute once
    #[test]
    fn test_rule_batch_once() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32)])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 100 })],
                super::PhysicalRuleExecutionStrategy::Once,
            )],
            PhysicalOptimizerConfig::new(5),
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 1, .. }));
        Ok(())
    }

    // make sure that fixed point cuts off when not transformed
    #[test]
    fn test_rule_batch_fixed_point() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32)])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 2 })],
                super::PhysicalRuleExecutionStrategy::FixedPoint(Some(4)),
            )],
            PhysicalOptimizerConfig::new(5),
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 2, .. }));
        Ok(())
    }

    // make sure that fixed point stops at maximum
    #[test]
    fn test_rule_batch_fixed_point_max() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32)])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 100 })],
                super::PhysicalRuleExecutionStrategy::FixedPoint(Some(4)),
            )],
            PhysicalOptimizerConfig::new(5),
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 4, .. }));
        Ok(())
    }

    // make sure that fixed point stops at max_passes
    #[test]
    fn test_rule_batch_fixed_point_max_passes() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32)])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 100 })],
                super::PhysicalRuleExecutionStrategy::FixedPoint(Some(7)),
            )],
            PhysicalOptimizerConfig::new(5),
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 5, .. }));
        Ok(())
    }

    // make sure that fixed point stops at max_passes without a limit
    #[test]
    fn test_rule_batch_fixed_point_no_limit() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32)])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 100 })],
                super::PhysicalRuleExecutionStrategy::FixedPoint(None),
            )],
            PhysicalOptimizerConfig::new(7),
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 7, .. }));
        Ok(())
    }
}
