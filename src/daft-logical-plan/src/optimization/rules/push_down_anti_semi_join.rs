use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_core::join::{JoinSide, JoinType};
use daft_dsl::{Column, Expr, ResolvedColumn, left_col};
use daft_schema::field::Field;

use super::OptimizerRule;
use crate::{
    LogicalPlan, LogicalPlanRef,
    ops::{Distinct, Filter, Join, Project, Sort, join::JoinPredicate},
};

/// Optimizer rule to push anti and semi joins down a plan tree.
///
/// Anti and semi joins work similarly to filters, where the left side is the input and the right side is the filter predicate.
/// They will only eliminate columns from the left side so, pushing them down will reduce the intermediate cardinalities of our data,
/// generally improving query performance.
///
/// One case where it may not improve query performance is when the join is pushed down below a filter.
/// However, we should still do it here so that we can potentially push it through joins below the filter, and then rely on PushDownFilter
/// to then move filters below this join again.
///
/// In the future, we should incorporate anti and semi join reordering into our general join reordering algorithm.
///
/// # Examples
///
/// ## Through a project
///
/// Before:
/// ```text
/// * Semi Join
/// |   on: x == a
/// |\
/// | * Scan
/// | |   schema: (a,)
/// |
/// * Project
/// |   projection: y AS x
/// |
/// * Scan
/// |   schema: (y,)
/// ```
///
/// After:
/// ```text
/// * Project
/// |   projection: y AS x
/// |
/// * Semi Join
/// |   on: y == a
/// |\
/// | * Scan
/// | |   schema: (a,)
/// |
/// * Scan
/// |   schema: (y,)
/// ```
///
/// ## Through an inner join
///
/// Before:
/// ```text
/// * Anti Join
/// |   on: x == a
/// |\
/// | * Scan
/// | |   schema: (a,)
/// |
/// * Inner Join
/// |   on: x == y
/// |\
/// | * Scan
/// | |   schema: (y,)
/// |
/// * Scan
/// |   schema: (x,)
/// ```
///
/// After:
/// ```text
/// * Inner Join
/// |   on: x == y
/// |\
/// | * Scan
/// | |   schema: (y,)
/// |
/// * Anti Join
/// |   on: x == a
/// |\
/// | * Scan
/// | |   schema: (a,)
/// |
/// * Scan
/// |   schema: (x,)
/// ```
#[derive(Debug)]
pub struct PushDownAntiSemiJoin {}

impl PushDownAntiSemiJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownAntiSemiJoin {
    fn try_optimize(&self, plan: LogicalPlanRef) -> DaftResult<Transformed<LogicalPlanRef>> {
        plan.transform_down(|node| {
            if let LogicalPlan::Join(Join {
                left,
                right,
                on,
                join_type,
                join_strategy,
                ..
            }) = node.as_ref()
                && matches!(join_type, JoinType::Anti | JoinType::Semi)
            {
                match left.as_ref() {
                    // We can push an anti/semi join down a project if it the columns the join uses (or its aliases) are present in the input of the project.
                    LogicalPlan::Project(Project {
                        input, projection, ..
                    }) => {
                        let Some(on_expr) = on.inner() else {
                            return Ok(Transformed::no(node));
                        };

                        let trivial_projections_map = projection
                            .iter()
                            .filter_map(|e| match e.as_ref() {
                                Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => {
                                    Some((name.to_string(), None))
                                }
                                Expr::Alias(child, new_name) => match child.as_ref() {
                                    Expr::Column(Column::Resolved(ResolvedColumn::Basic(
                                        original_name,
                                    ))) => Some((
                                        new_name.to_string(),
                                        Some(original_name.to_string()),
                                    )),
                                    _ => None,
                                },
                                _ => None,
                            })
                            .collect::<HashMap<_, _>>();

                        let mut renaming_map = HashMap::new();
                        let mut can_push_down = true;

                        on_expr
                            .apply(|e| {
                                if let Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(
                                    field,
                                    JoinSide::Left,
                                ))) = e.as_ref()
                                {
                                    match trivial_projections_map.get(&field.name) {
                                        Some(Some(original_name)) => {
                                            renaming_map.insert(field.name.clone(), original_name);
                                        }
                                        Some(None) => {}
                                        None => {
                                            can_push_down = false;
                                            return Ok(TreeNodeRecursion::Stop);
                                        }
                                    }
                                }

                                Ok(TreeNodeRecursion::Continue)
                            })
                            .unwrap();

                        if can_push_down {
                            let new_on_expr = on_expr
                                .clone()
                                .transform(|e| {
                                    if let Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(
                                        field,
                                        JoinSide::Left,
                                    ))) = e.as_ref()
                                        && let Some(original_name) = renaming_map.get(&field.name)
                                    {
                                        Ok(Transformed::yes(left_col(Field::new(
                                            (*original_name).clone(),
                                            field.dtype.clone(),
                                        ))))
                                    } else {
                                        Ok(Transformed::no(e))
                                    }
                                })
                                .unwrap()
                                .data;

                            let new_on = JoinPredicate::try_new(Some(new_on_expr))?;

                            let new_child = Join::try_new(
                                input.clone(),
                                right.clone(),
                                new_on,
                                *join_type,
                                *join_strategy,
                            )?
                            .into();

                            return Ok(Transformed::yes(Arc::new(
                                left.with_new_children(&[new_child]),
                            )));
                        }
                    }
                    // If the anti/semi join only uses columns from one side of the join, we can push it down that side
                    LogicalPlan::Join(Join {
                        left: child_left,
                        right: child_right,
                        join_type: JoinType::Inner,
                        ..
                    }) => {
                        let mut requires_left = false;
                        let mut requires_right = false;

                        if let Some(on_expr) = on.inner() {
                            on_expr
                                .apply(|e| {
                                    if let Expr::Column(Column::Resolved(
                                        ResolvedColumn::JoinSide(field, JoinSide::Left),
                                    )) = e.as_ref()
                                    {
                                        if child_left.schema().has_field(&field.name) {
                                            requires_left = true;
                                        }
                                        if child_right.schema().has_field(&field.name) {
                                            requires_right = true;
                                        }
                                    }

                                    Ok(TreeNodeRecursion::Continue)
                                })
                                .unwrap();
                        }

                        match (requires_left, requires_right) {
                            // requires only left side of child join, push down left
                            (true, false) => {
                                let new_child_left = Join::try_new(
                                    child_left.clone(),
                                    right.clone(),
                                    on.clone(),
                                    *join_type,
                                    *join_strategy,
                                )?
                                .into();

                                return Ok(Transformed::yes(Arc::new(
                                    left.with_new_children(&[new_child_left, child_right.clone()]),
                                )));
                            }
                            // requires only right side of child join, push down right
                            (false, true) => {
                                let new_child_right = Join::try_new(
                                    child_right.clone(),
                                    right.clone(),
                                    on.clone(),
                                    *join_type,
                                    *join_strategy,
                                )?
                                .into();

                                return Ok(Transformed::yes(Arc::new(
                                    left.with_new_children(&[child_left.clone(), new_child_right]),
                                )));
                            }
                            _ => {}
                        }
                    }
                    // Anti/semi join cannot be pushed down a non inner join.
                    LogicalPlan::Join(Join {
                        left: _,
                        right: _,
                        join_type: _,
                        ..
                    }) => {
                        return Ok(Transformed::no(node));
                    }
                    // Anti/semi join can be trivially pushed down these ops
                    LogicalPlan::Filter(Filter { input, .. })
                    | LogicalPlan::Sort(Sort { input, .. })
                    | LogicalPlan::Distinct(Distinct { input, .. }) => {
                        let new_child = Join::try_new(
                            input.clone(),
                            right.clone(),
                            on.clone(),
                            *join_type,
                            *join_strategy,
                        )?
                        .into();

                        return Ok(Transformed::yes(Arc::new(
                            left.with_new_children(&[new_child]),
                        )));
                    }
                    LogicalPlan::Limit(_)
                    | LogicalPlan::Offset(_)
                    | LogicalPlan::TopN(..)
                    | LogicalPlan::Sample(..)
                    | LogicalPlan::Explode(..)
                    | LogicalPlan::Shard(..)
                    | LogicalPlan::UDFProject(..)
                    | LogicalPlan::Unpivot(..)
                    | LogicalPlan::Pivot(..)
                    | LogicalPlan::Aggregate(..)
                    | LogicalPlan::Intersect(..)
                    | LogicalPlan::Union(..)
                    | LogicalPlan::Sink(..)
                    | LogicalPlan::MonotonicallyIncreasingId(..)
                    | LogicalPlan::SubqueryAlias(..)
                    | LogicalPlan::Window(..)
                    | LogicalPlan::Source(_)
                    | LogicalPlan::Repartition(_)
                    | LogicalPlan::IntoBatches(_)
                    | LogicalPlan::Concat(_)
                    | LogicalPlan::VLLMProject(..) => {}
                }
            }

            Ok(Transformed::no(node))
        })
    }
}

#[cfg(test)]
mod tests {
    use daft_dsl::{lit, unresolved_col};
    use daft_schema::dtype::DataType;
    use rstest::rstest;

    use super::*;
    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
    };

    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(PushDownAntiSemiJoin::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[rstest]
    fn join_commutes_with_project_simple(
        #[values(JoinType::Anti, JoinType::Semi)] join_type: JoinType,
    ) -> DaftResult<()> {
        let left_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("y", DataType::Int64),
        ]));
        let right_scan_node =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        let plan = left_scan_node
            .select(vec![
                unresolved_col("x"),
                unresolved_col("x")
                    .add(unresolved_col("y"))
                    .alias("x_plus_y"),
            ])?
            .join(
                right_scan_node.clone(),
                Some(unresolved_col("x").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .build();

        let expected = left_scan_node
            .join(
                right_scan_node,
                Some(unresolved_col("x").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .select(vec![
                unresolved_col("x"),
                unresolved_col("x")
                    .add(unresolved_col("y"))
                    .alias("x_plus_y"),
            ])?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    #[rstest]
    fn join_commutes_with_project_aliased(
        #[values(JoinType::Anti, JoinType::Semi)] join_type: JoinType,
    ) -> DaftResult<()> {
        let left_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("y", DataType::Int64),
        ]));
        let right_scan_node =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        let plan = left_scan_node
            .select(vec![
                unresolved_col("x").alias("z"),
                unresolved_col("x")
                    .add(unresolved_col("y"))
                    .alias("x_plus_y"),
            ])?
            .join(
                right_scan_node.clone(),
                Some(unresolved_col("z").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .build();

        let expected = left_scan_node
            .join(
                right_scan_node,
                Some(unresolved_col("x").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .select(vec![
                unresolved_col("x").alias("z"),
                unresolved_col("x")
                    .add(unresolved_col("y"))
                    .alias("x_plus_y"),
            ])?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    #[rstest]
    fn join_does_not_commute_with_nontrivial_project(
        #[values(JoinType::Anti, JoinType::Semi)] join_type: JoinType,
    ) -> DaftResult<()> {
        let left_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("y", DataType::Int64),
        ]));
        let right_scan_node =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        let plan = left_scan_node
            .select(vec![
                unresolved_col("x"),
                unresolved_col("x")
                    .add(unresolved_col("y"))
                    .alias("x_plus_y"),
            ])?
            .join(
                right_scan_node.clone(),
                Some(unresolved_col("x_plus_y").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    #[rstest]
    fn join_commutes_with_inner_join_left(
        #[values(JoinType::Anti, JoinType::Semi)] join_type: JoinType,
    ) -> DaftResult<()> {
        let left_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("y", DataType::Int64),
        ]));
        let right_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("z", DataType::Int64),
        ]));

        let filtering_scan_node =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        let plan = left_scan_node
            .join(
                right_scan_node.clone(),
                None,
                vec!["x".to_string()],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .join(
                filtering_scan_node.clone(),
                Some(unresolved_col("y").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .build();

        let expected = left_scan_node
            .join(
                filtering_scan_node.clone(),
                Some(unresolved_col("y").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .join(
                right_scan_node.clone(),
                None,
                vec!["x".to_string()],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    #[rstest]
    fn join_commutes_with_inner_join_right(
        #[values(JoinType::Anti, JoinType::Semi)] join_type: JoinType,
    ) -> DaftResult<()> {
        let left_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("y", DataType::Int64),
        ]));
        let right_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("z", DataType::Int64),
        ]));

        let filtering_scan_node =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        let plan = left_scan_node
            .join(
                right_scan_node.clone(),
                None,
                vec!["x".to_string()],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .join(
                filtering_scan_node.clone(),
                Some(unresolved_col("z").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .build();

        let expected = left_scan_node
            .join(
                right_scan_node
                    .join(
                        filtering_scan_node.clone(),
                        Some(unresolved_col("z").eq(unresolved_col("a"))),
                        vec![],
                        join_type,
                        None,
                        Default::default(),
                    )?
                    .build(),
                None,
                vec!["x".to_string()],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    #[rstest]
    fn join_does_not_commute_with_inner_join_common_col(
        #[values(JoinType::Anti, JoinType::Semi)] join_type: JoinType,
    ) -> DaftResult<()> {
        let left_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("y", DataType::Int64),
        ]));
        let right_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("z", DataType::Int64),
        ]));

        let filtering_scan_node =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        let plan = left_scan_node
            .join(
                right_scan_node.clone(),
                None,
                vec!["x".to_string()],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .join(
                filtering_scan_node.clone(),
                Some(unresolved_col("x").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    #[rstest]
    fn join_commutes_with_filter(
        #[values(JoinType::Anti, JoinType::Semi)] join_type: JoinType,
    ) -> DaftResult<()> {
        let left_scan_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("x", DataType::Int64),
            Field::new("y", DataType::Int64),
        ]));
        let right_scan_node =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        let plan = left_scan_node
            .filter(
                unresolved_col("x")
                    .gt(lit(1))
                    .and(unresolved_col("y").lt(lit(2))),
            )?
            .join(
                right_scan_node.clone(),
                Some(unresolved_col("x").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .build();

        let expected = left_scan_node
            .join(
                right_scan_node.clone(),
                Some(unresolved_col("x").eq(unresolved_col("a"))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .filter(
                unresolved_col("x")
                    .gt(lit(1))
                    .and(unresolved_col("y").lt(lit(2))),
            )?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }
}
