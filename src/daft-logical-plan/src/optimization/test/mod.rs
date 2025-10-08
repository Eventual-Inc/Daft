use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::{
    LogicalPlan,
    optimization::{Optimizer, OptimizerConfig, optimizer::RuleBatch},
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
/// the provided plan with said optimizer, and compares the optimized plan with
/// the provided expected plan.
pub fn assert_optimized_plan_with_rules_repr_eq(
    plan: Arc<LogicalPlan>,
    expected_repr: &str,
    rule_batches: Vec<RuleBatch>,
) -> DaftResult<()> {
    let unoptimized_plan = plan.clone();
    let optimized_plan = optimize_with_rules(plan, rule_batches)?.repr_indent();

    assert_eq!(
        optimized_plan,
        expected_repr,
        "\n\nOptimized plan not equal to expected.\n\nBefore Optimization:\n{}\n\nOptimized:\n{}\n\nExpected:\n{}",
        unoptimized_plan.repr_indent(),
        optimized_plan,
        expected_repr
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

pub fn optimize_with_rules(
    plan: Arc<LogicalPlan>,
    rule_batches: Vec<RuleBatch>,
) -> DaftResult<Arc<LogicalPlan>> {
    let config = OptimizerConfig::default();
    let optimizer = Optimizer::with_rule_batches(rule_batches, config);

    match optimizer.optimize(plan, |_, _, _, _, _| {}) {
        Ok(data) => Ok(data),
        Err(e) => Err(e),
    }
}
