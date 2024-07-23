use common_error::DaftResult;
use common_treenode::{Transformed, TransformedResult};

use crate::PhysicalPlanRef;

use super::rules::{
    drop_repartition::DropRepartitionPhysical, reorder_partition_keys::ReorderPartitionKeys,
};

pub trait PhysicalOptimizerRule {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>>;
}

pub enum PhysicalRuleExecutionStrategy {
    // Apply the batch of rules only once.
    Once,
    // Apply the batch of rules multiple times, to a fixed-point or until the max
    // passes is hit.
    // If parametrized by Some(n), the batch of rules will be run a maximum
    // of n passes; if None, the number of passes is capped by the max passes argument.
    #[allow(dead_code)]
    FixedPoint(Option<usize>),
}

pub struct PhysicalOptimizerRuleBatch {
    rules: Vec<Box<dyn PhysicalOptimizerRule>>,
    strategy: PhysicalRuleExecutionStrategy,
}

impl PhysicalOptimizerRuleBatch {
    pub fn new(
        rules: Vec<Box<dyn PhysicalOptimizerRule>>,
        strategy: PhysicalRuleExecutionStrategy,
    ) -> Self {
        PhysicalOptimizerRuleBatch { rules, strategy }
    }

    fn optimize_once(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>> {
        self.rules
            .iter()
            .try_fold(Transformed::no(plan), |plan, rule| {
                plan.transform_data(|p| rule.rewrite(p))
            })
    }

    pub fn optimize(
        &self,
        plan: PhysicalPlanRef,
        max_passes: usize,
    ) -> DaftResult<PhysicalPlanRef> {
        match self.strategy {
            PhysicalRuleExecutionStrategy::Once => self.optimize_once(plan).data(),
            PhysicalRuleExecutionStrategy::FixedPoint(passes) => {
                let passes = passes.unwrap_or(max_passes);
                let passes = std::cmp::min(passes, max_passes);
                let mut plan = plan;
                for _ in 0..passes {
                    let transformed_plan = self.optimize_once(plan.clone())?;
                    if !transformed_plan.transformed {
                        break;
                    }
                    plan = transformed_plan.data;
                }
                Ok(plan)
            }
        }
    }
}

pub struct PhysicalOptimizer {
    rule_batches: Vec<PhysicalOptimizerRuleBatch>,
    max_passes: usize,
}

impl PhysicalOptimizer {
    #[allow(dead_code)] // used in test
    pub fn new(rule_batches: Vec<PhysicalOptimizerRuleBatch>, max_passes: usize) -> Self {
        PhysicalOptimizer {
            rule_batches,
            max_passes,
        }
    }

    pub fn optimize(&self, mut plan: PhysicalPlanRef) -> DaftResult<PhysicalPlanRef> {
        for batch in self.rule_batches.iter() {
            plan = batch.optimize(plan, self.max_passes)?;
        }
        Ok(plan)
    }
}

impl Default for PhysicalOptimizer {
    fn default() -> Self {
        PhysicalOptimizer {
            rule_batches: vec![PhysicalOptimizerRuleBatch::new(
                vec![
                    Box::new(ReorderPartitionKeys {}),
                    Box::new(DropRepartitionPhysical {}),
                ],
                PhysicalRuleExecutionStrategy::Once,
            )],
            max_passes: 5,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{assert_matches::assert_matches, sync::Arc};

    use common_error::DaftResult;
    use common_treenode::Transformed;
    use daft_core::{
        datatypes::Field,
        schema::{Schema, SchemaRef},
    };

    use crate::{
        partitioning::UnknownClusteringConfig,
        physical_ops::{EmptyScan, Limit},
        ClusteringSpec, PhysicalPlan, PhysicalPlanRef,
    };

    use super::{PhysicalOptimizer, PhysicalOptimizerRule, PhysicalOptimizerRuleBatch};

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
            Arc::new(Schema::new(vec![Field::new(
                "a",
                daft_core::DataType::Int32,
            )])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 100 })],
                super::PhysicalRuleExecutionStrategy::Once,
            )],
            5,
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 1, .. }));
        Ok(())
    }

    // make sure that fixed point cuts off when not transformed
    #[test]
    fn test_rule_batch_fixed_point() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![Field::new(
                "a",
                daft_core::DataType::Int32,
            )])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 2 })],
                super::PhysicalRuleExecutionStrategy::FixedPoint(Some(4)),
            )],
            5,
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 2, .. }));
        Ok(())
    }

    // make sure that fixed point stops at maximum
    #[test]
    fn test_rule_batch_fixed_point_max() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![Field::new(
                "a",
                daft_core::DataType::Int32,
            )])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 100 })],
                super::PhysicalRuleExecutionStrategy::FixedPoint(Some(4)),
            )],
            5,
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 4, .. }));
        Ok(())
    }

    // make sure that fixed point stops at max_passes
    #[test]
    fn test_rule_batch_fixed_point_max_passes() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![Field::new(
                "a",
                daft_core::DataType::Int32,
            )])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 100 })],
                super::PhysicalRuleExecutionStrategy::FixedPoint(Some(7)),
            )],
            5,
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 5, .. }));
        Ok(())
    }

    // make sure that fixed point stops at max_passes without a limit
    #[test]
    fn test_rule_batch_fixed_point_no_limit() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![Field::new(
                "a",
                daft_core::DataType::Int32,
            )])?),
            1,
        );

        let plan = PhysicalPlan::Limit(Limit::new(plan, 0, true, 1));
        let optimizer = PhysicalOptimizer::new(
            vec![PhysicalOptimizerRuleBatch::new(
                vec![Box::new(CountingRule { cutoff: 100 })],
                super::PhysicalRuleExecutionStrategy::FixedPoint(None),
            )],
            5,
        );
        let plan = optimizer.optimize(plan.arced())?;
        assert_matches!(plan.as_ref(), PhysicalPlan::Limit(Limit { limit: 5, .. }));
        Ok(())
    }
}
