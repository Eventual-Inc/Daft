use std::sync::Arc;

use common_error::DaftResult;

use crate::{
    optimization::{optimizer::RuleBatch, Optimizer},
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
    let optimizer = Optimizer::with_rule_batches(rule_batches, Default::default());
    let optimized_plan = optimizer
        .optimize_with_rules(optimizer.rule_batches[0].rules.as_slice(), plan.clone())?
        .data;
    assert_eq!(
        optimized_plan,
        expected,
        "\n\nOptimized plan not equal to expected.\n\nBefore Optimization:\n{}\n\nOptimized:\n{}\n\nExpected:\n{}",
        plan.repr_ascii(false),
        optimized_plan.repr_ascii(false),
        expected.repr_ascii(false)
    );

    Ok(())
}
