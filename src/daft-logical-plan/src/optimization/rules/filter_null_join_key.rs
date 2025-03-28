use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_algebra::boolean::combine_conjunction;
use daft_core::join::JoinType;

use super::OptimizerRule;
use crate::{
    ops::{Filter, Join},
    LogicalPlan,
};

/// Optimization rule for filtering out nulls from join keys.
///
/// When a join will always discard null keys from a join side,
/// this rule inserts a filter before that side to remove rows where a join key is null.
/// This reduces the cardinality of the tables before a join to improve join performance,
/// and can also be pushed down with other rules to reduce source and intermediate output sizes.
///
/// # Example
/// ```sql
/// SELECT * FROM left JOIN right ON left.x = right.y
/// ```
/// turns into
/// ```sql
/// SELECT *
/// FROM (SELECT * FROM left WHERE x IS NOT NULL) AS non_null_left
/// JOIN (SELECT * FROM right WHERE x IS NOT NULL) AS non_null_right
/// ON non_null_left.x = non_null_right.y
/// ```
///
/// So if `left` was:
/// ```
/// ╭───────╮
/// │ x     │
/// │ ---   │
/// │ Int64 │
/// ╞═══════╡
/// │ 1     │
/// ├╌╌╌╌╌╌╌┤
/// │ 2     │
/// ├╌╌╌╌╌╌╌┤
/// │ None  │
/// ╰───────╯
/// ```
/// And `right` was:
/// ```
/// ╭───────╮
/// │ y     │
/// │ ---   │
/// │ Int64 │
/// ╞═══════╡
/// │ 1     │
/// ├╌╌╌╌╌╌╌┤
/// │ None  │
/// ├╌╌╌╌╌╌╌┤
/// │ None  │
/// ╰───────╯
/// ```
/// the original query would join on all rows, whereas the new query would first filter out null rows and join on the following:
///
/// `non_null_left`:
/// ```
/// ╭───────╮
/// │ x     │
/// │ ---   │
/// │ Int64 │
/// ╞═══════╡
/// │ 1     │
/// ├╌╌╌╌╌╌╌┤
/// │ 2     │
/// ╰───────╯
/// ```
/// `non_null_right`:
/// ```
/// ╭───────╮
/// │ y     │
/// │ ---   │
/// │ Int64 │
/// ╞═══════╡
/// │ 1     │
/// ╰───────╯
/// ```
#[derive(Default, Debug)]
pub struct FilterNullJoinKey {}

impl FilterNullJoinKey {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for FilterNullJoinKey {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform(|node| {
            if let LogicalPlan::Join(Join {
                left,
                right,
                on,
                join_type,
                ..
            }) = node.as_ref()
            {
                let (_, left_on, right_on, null_equals_nulls) = on.split_eq_preds();

                let (can_filter_left, can_filter_right) = match join_type {
                    JoinType::Inner => (true, true),
                    JoinType::Left => (false, true),
                    JoinType::Right => (true, false),
                    JoinType::Outer => (false, false),
                    JoinType::Anti => (false, true),
                    JoinType::Semi => (true, true),
                };

                let left_null_pred = if can_filter_left {
                    combine_conjunction(
                        null_equals_nulls
                            .iter()
                            .zip(left_on)
                            .filter(|(null_eq_null, _)| !*null_eq_null)
                            .map(|(_, left_key)| left_key.is_null().not()),
                    )
                } else {
                    None
                };

                let right_null_pred = if can_filter_right {
                    combine_conjunction(
                        null_equals_nulls
                            .iter()
                            .zip(right_on)
                            .filter(|(null_eq_null, _)| !*null_eq_null)
                            .map(|(_, right_key)| right_key.is_null().not()),
                    )
                } else {
                    None
                };

                if left_null_pred.is_none() && right_null_pred.is_none() {
                    Ok(Transformed::no(node.clone()))
                } else {
                    let new_left = if let Some(pred) = left_null_pred {
                        Arc::new(LogicalPlan::Filter(Filter::try_new(left.clone(), pred)?))
                    } else {
                        left.clone()
                    };

                    let new_right = if let Some(pred) = right_null_pred {
                        Arc::new(LogicalPlan::Filter(Filter::try_new(right.clone(), pred)?))
                    } else {
                        right.clone()
                    };

                    let new_join = Arc::new(node.with_new_children(&[new_left, new_right]));

                    Ok(Transformed::yes(new_join))
                }
            } else {
                Ok(Transformed::no(node))
            }
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
            rules::filter_null_join_key::FilterNullJoinKey,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
    };

    /// Helper that creates an optimizer with the FilterNullJoinKey rule registered, optimizes
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
                vec![Box::new(FilterNullJoinKey::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[test]
    fn filter_keys_basic() -> DaftResult<()> {
        let left_scan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]));

        let right_scan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("c", DataType::Int64),
            Field::new("d", DataType::Utf8),
        ]));

        let plan = left_scan
            .join(
                right_scan.clone(),
                unresolved_col("a").eq(unresolved_col("c")).into(),
                vec![],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .build();

        let expected = left_scan
            .filter(unresolved_col("a").is_null().not())?
            .clone()
            .join(
                right_scan.filter(unresolved_col("c").is_null().not())?,
                unresolved_col("a").eq(unresolved_col("c")).into(),
                vec![],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    #[test]
    fn filter_keys_null_equals_nulls() -> DaftResult<()> {
        let left_scan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
            Field::new("c", DataType::Boolean),
        ]));

        let right_scan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("d", DataType::Int64),
            Field::new("e", DataType::Utf8),
            Field::new("f", DataType::Boolean),
        ]));

        let plan = left_scan
            .join(
                right_scan.clone(),
                (unresolved_col("a").eq(unresolved_col("d")))
                    .and(unresolved_col("b").eq_null_safe(unresolved_col("e")))
                    .and(unresolved_col("c").eq(unresolved_col("f")))
                    .into(),
                vec![],
                JoinType::Left,
                None,
                Default::default(),
            )?
            .build();

        let expected_predicate = unresolved_col("d")
            .is_null()
            .not()
            .and(unresolved_col("f").is_null().not());

        let expected = left_scan
            .clone()
            .join(
                right_scan.filter(expected_predicate)?,
                (unresolved_col("a").eq(unresolved_col("d")))
                    .and(unresolved_col("b").eq_null_safe(unresolved_col("e")))
                    .and(unresolved_col("c").eq(unresolved_col("f")))
                    .into(),
                vec![],
                JoinType::Left,
                None,
                Default::default(),
            )?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }
}
