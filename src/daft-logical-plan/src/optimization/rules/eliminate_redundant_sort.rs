use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{Expr, ExprRef, functions::scalar::ScalarFn};

use super::OptimizerRule;
use crate::LogicalPlan;

/// Optimization rule for eliminating a `Sort` whose ordering is fully overwritten
/// by a later `Sort`.
///
/// A `Sort` establishes a total ordering over its output. When another `Sort` is
/// applied further up the plan, it re-establishes the ordering from scratch, which
/// makes the earlier (inner) `Sort` redundant work that can be dropped.
///
/// The rule looks through row-wise, order-insensitive operators (`Project` and
/// `Filter`) that sit between the two sorts: dropping the inner sort does not change
/// the multiset of rows these operators produce, and the outer sort re-orders the
/// final result anyway. It intentionally stops at order-sensitive operators such as
/// `Limit`, `Offset`, `TopN`, `Sample`, and `MonotonicallyIncreasingId`, where the
/// inner ordering is observable and therefore must be preserved. A `Project` or
/// `Filter` that still embeds a `monotonically_increasing_id()` expression is
/// order-sensitive in the same way — `DetectMonotonicId` rewrites those into the
/// operator form, but it runs after this rule — so the look-through stops there too.
///
/// ```text
/// Sort(a) <- Sort(b) <- X                        => Sort(a) <- X
/// Sort(a) <- Project <- Sort(b) <- X             => Sort(a) <- Project <- X
/// Sort(a) <- Sort(b) <- Sort(c) <- X             => Sort(a) <- X
/// Sort(a) <- Limit   <- Sort(b) <- X             => unchanged
/// Sort(a) <- Project(monotonic_id) <- Sort(b) <- X => unchanged
/// ```
#[derive(Default, Debug)]
pub struct EliminateRedundantSort {}

impl EliminateRedundantSort {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateRedundantSort {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| {
            let LogicalPlan::Sort(sort) = node.as_ref() else {
                return Ok(Transformed::no(node));
            };
            match remove_redundant_sorts(&sort.input) {
                Some(new_input) => Ok(Transformed::yes(
                    node.with_new_children(&[new_input]).into(),
                )),
                None => Ok(Transformed::no(node)),
            }
        })
    }
}

/// Returns true if the expression contains a `monotonically_increasing_id()`
/// call. Until `DetectMonotonicId` (which runs after this rule) rewrites such
/// expressions into a `MonotonicallyIncreasingId` operator, they sit embedded in
/// `Project`/`Filter` nodes, whose row order then determines the ids assigned.
fn contains_monotonic_id(expr: &ExprRef) -> bool {
    match expr.as_ref() {
        Expr::ScalarFn(ScalarFn::Builtin(func)) if func.name() == "monotonically_increasing_id" => {
            true
        }
        _ => expr.children().iter().any(contains_monotonic_id),
    }
}

fn contains_monotonic_id_exprs(exprs: &[ExprRef]) -> bool {
    exprs.iter().any(contains_monotonic_id)
}

/// Walks down from an (outer) `Sort`'s input, removing any inner `Sort`s whose
/// ordering the outer sort makes redundant.
///
/// Returns `Some(new_plan)` if at least one inner sort was removed, or `None` if
/// there was nothing to eliminate.
fn remove_redundant_sorts(plan: &Arc<LogicalPlan>) -> Option<Arc<LogicalPlan>> {
    match plan.as_ref() {
        // A redundant inner sort: drop it, and keep collapsing any further sorts below it.
        LogicalPlan::Sort(inner) => {
            Some(remove_redundant_sorts(&inner.input).unwrap_or_else(|| inner.input.clone()))
        }
        // Order-insensitive, row-wise operators: safe to look through, unless they
        // embed a monotonically_increasing_id() expression, which observes the row
        // order produced below (see contains_monotonic_id).
        LogicalPlan::Project(project) => {
            if contains_monotonic_id_exprs(&project.projection) {
                return None;
            }
            remove_redundant_sorts(&project.input)
                .map(|new_child| plan.with_new_children(&[new_child]).into())
        }
        LogicalPlan::Filter(filter) => {
            if contains_monotonic_id(&filter.predicate) {
                return None;
            }
            remove_redundant_sorts(&filter.input)
                .map(|new_child| plan.with_new_children(&[new_child]).into())
        }
        // Any other operator may depend on row order; stop here to stay conservative.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{lit, unresolved_col};
    use daft_functions::monotonically_increasing_id::monotonically_increasing_id;

    use crate::{
        LogicalPlan,
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::eliminate_redundant_sort::EliminateRedundantSort,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
    };

    /// Helper that creates an optimizer with the `EliminateRedundantSort` rule registered,
    /// optimizes the provided plan, and compares the result against the expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(EliminateRedundantSort::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    /// Two back-to-back sorts collapse to just the outer sort.
    ///
    /// Sort(b) <- Sort(a) <- Source => Sort(b) <- Source
    #[test]
    fn eliminates_directly_adjacent_sort() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .sort(vec![unresolved_col("a")], vec![false], vec![false])?
            .sort(vec![unresolved_col("b")], vec![false], vec![false])?
            .build();
        let expected = dummy_scan_node(scan_op)
            .sort(vec![unresolved_col("b")], vec![false], vec![false])?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// A chain of three sorts collapses to just the outer sort in a single pass.
    ///
    /// Sort(c) <- Sort(b) <- Sort(a) <- Source => Sort(c) <- Source
    #[test]
    fn eliminates_chain_of_sorts() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .sort(vec![unresolved_col("a")], vec![false], vec![false])?
            .sort(vec![unresolved_col("b")], vec![false], vec![false])?
            .sort(vec![unresolved_col("c")], vec![false], vec![false])?
            .build();
        let expected = dummy_scan_node(scan_op)
            .sort(vec![unresolved_col("c")], vec![false], vec![false])?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// The inner sort is removed even when a row-wise `Filter` sits between the two sorts.
    ///
    /// Sort(b) <- Filter <- Sort(a) <- Source => Sort(b) <- Filter <- Source
    #[test]
    fn looks_through_filter() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .sort(vec![unresolved_col("a")], vec![false], vec![false])?
            .filter(unresolved_col("a").gt(lit(1)))?
            .sort(vec![unresolved_col("b")], vec![false], vec![false])?
            .build();
        let expected = dummy_scan_node(scan_op)
            .filter(unresolved_col("a").gt(lit(1)))?
            .sort(vec![unresolved_col("b")], vec![false], vec![false])?
            .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// A lone sort with no sort below it is left untouched.
    #[test]
    fn keeps_single_sort() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let plan = dummy_scan_node(scan_op)
            .sort(vec![unresolved_col("a")], vec![false], vec![false])?
            .build();
        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }

    /// An order-sensitive `Limit` between the sorts blocks elimination: the inner sort
    /// determines which rows the limit keeps, so it must be preserved.
    ///
    /// Sort(b) <- Limit <- Sort(a) <- Source => unchanged
    #[test]
    fn keeps_sort_blocked_by_limit() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let plan = dummy_scan_node(scan_op)
            .sort(vec![unresolved_col("a")], vec![false], vec![false])?
            .limit(5, false)?
            .sort(vec![unresolved_col("b")], vec![false], vec![false])?
            .build();
        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }

    /// A `Project` embedding a `monotonically_increasing_id()` expression is
    /// order-sensitive: the ids it assigns depend on the order rows arrive in, and
    /// `DetectMonotonicId` (which runs after this rule) rewrites the expression into
    /// the `MonotonicallyIncreasingId` operator that this rule already stops at. The
    /// look-through must therefore stop here and keep the inner sort.
    ///
    /// Sort(b) <- Project(monotonic_id) <- Sort(a) <- Source => unchanged
    #[test]
    fn keeps_sort_blocked_by_monotonic_id_projection() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let plan = dummy_scan_node(scan_op)
            .sort(vec![unresolved_col("a")], vec![false], vec![false])?
            .select(vec![unresolved_col("b"), monotonically_increasing_id()])?
            .sort(vec![unresolved_col("b")], vec![false], vec![false])?
            .build();
        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }
}
