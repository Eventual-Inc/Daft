use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_treenode::Transformed;

use crate::{
    optimization::{
        optimizer::{RuleBatch, RuleExecutionStrategy},
        Optimizer, OptimizerConfig,
    },
    LogicalPlan,
};

/// Helper that creates an optimizer with the provided rules registered, optimizes
/// the provided plan with said optimizer, and compares the optimized plan with
/// the provided expected plan.
pub fn assert_optimized_plan_with_rules_eq(
    plan: Arc<LogicalPlan>,
    expected: Arc<LogicalPlan>,
    rule_batches: Vec<RuleBatch>,
) -> DaftResult<()> {
    let unoptimized_plan = plan.clone();
    let optimized_plan = optimize_with_rules(plan, rule_batches)?;

    assert_eq!(
        optimized_plan,
        expected,
        "\n\nOptimized plan not equal to expected.\n\nBefore Optimization:\n{}\n\nOptimized:\n{}\n\nExpected:\n{}",
        unoptimized_plan.repr_ascii(false),
        optimized_plan.repr_ascii(false),
        expected.repr_ascii(false)
    );

    Ok(())
}

/// Helper that creates an optimizer with the provided rules registered, optimizes
/// the provided plan with said optimizer, and expect to return the expected error
/// during the optimization process.
pub fn assert_optimized_plan_with_rules_err(
    plan: Arc<LogicalPlan>,
    err: DaftError,
    rule_batches: Vec<RuleBatch>,
) -> DaftResult<()> {
    let optimized_plan = optimize_with_rules(plan, rule_batches);
    assert!(optimized_plan.is_err());
    assert_eq!(optimized_plan.unwrap_err().to_string(), err.to_string());

    Ok(())
}

fn optimize_with_rules(
    plan: Arc<LogicalPlan>,
    rule_batches: Vec<RuleBatch>,
) -> DaftResult<Arc<LogicalPlan>> {
    let config = OptimizerConfig::default();
    let default_max_passes = config.default_max_optimizer_passes;
    let optimizer = Optimizer::with_rule_batches(rule_batches, config);
    let rule_batch = &optimizer.rule_batches[0];
    let max_passes = match rule_batch.strategy {
        RuleExecutionStrategy::Once => 1,
        RuleExecutionStrategy::FixedPoint(max_passes) => max_passes.unwrap_or(default_max_passes),
    };

    (0..max_passes).try_fold(plan, |plan, pass| -> DaftResult<Arc<LogicalPlan>> {
        match optimizer.optimize_with_rules(rule_batch.rules.as_slice(), plan) {
            Ok(Transformed { data, .. }) => Ok(data),
            Err(e) => Err(e),
        }
    })
}
