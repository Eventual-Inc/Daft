use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{optimization::requires_computation, resolved_col, Expr};
use indexmap::IndexSet;

use super::OptimizerRule;
use crate::{
    ops::{Aggregate, Project},
    LogicalPlan,
};

/// Rewrite rule for lifting expressions that can be done in a project out of an aggregation.
/// After a pass of this rule, the top level expressions in each aggregate should all be aliases or agg exprs.
///
///The logical to physical plan translation currently assumes that expressions are lifted out of aggregations,
/// so this rule must be run to rewrite the plan into a valid state.
///
/// # Examples
///
/// ### Global Agg
/// Input: `Agg [sum("x") + sum("y")] <- Scan`
///
/// Output: `Project [col("sum(x)") + col("sum(y)")] <- Agg [sum("x"), sum("y")] <- Scan`
///
/// ### Groupby Agg
/// Input: `Agg [groupby="key", sum("x") + sum("y")] <- Scan`
///
/// Output: `Project ["key", col("sum(x)") + col("sum(y)")] <- Agg [groupby="key", sum("x"), sum("y")] <- Scan`
#[derive(Default, Debug)]
pub struct LiftProjectFromAgg {}

impl LiftProjectFromAgg {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for LiftProjectFromAgg {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform(|node| {
            let LogicalPlan::Aggregate(aggregate) = node.as_ref() else {
                return Ok(Transformed::no(node));
            };

            let schema = node.schema();

            let mut agg_exprs = IndexSet::new();

            let lifted_exprs = aggregate
                .aggregations
                .iter()
                .map(|expr| {
                    let name = expr.name();
                    let new_expr = expr
                        .clone()
                        .transform_down(|e| {
                            if matches!(e.as_ref(), Expr::Agg(_)) {
                                let id = e.semantic_id(schema.as_ref()).id;
                                agg_exprs.insert(e.alias(id.clone()));
                                Ok(Transformed::yes(resolved_col(id)))
                            } else {
                                Ok(Transformed::no(e))
                            }
                        })
                        .unwrap()
                        .data;

                    if new_expr.name() != name {
                        new_expr.alias(name)
                    } else {
                        new_expr
                    }
                })
                .collect::<Vec<_>>();

            if lifted_exprs
                .iter()
                .any(|expr| requires_computation(expr.as_ref()))
            {
                let project_exprs = aggregate
                    .groupby
                    .iter()
                    .map(|e| resolved_col(e.name()))
                    .chain(lifted_exprs)
                    .collect::<Vec<_>>();

                let new_aggregate = Arc::new(LogicalPlan::Aggregate(Aggregate::try_new(
                    aggregate.input.clone(),
                    agg_exprs.into_iter().collect(),
                    aggregate.groupby.clone(),
                )?));

                let new_project = Arc::new(LogicalPlan::Project(Project::try_new(
                    new_aggregate,
                    project_exprs,
                )?));

                Ok(Transformed::yes(new_project))
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
    use daft_dsl::unresolved_col;
    use daft_schema::{dtype::DataType, field::Field};

    use super::LiftProjectFromAgg;
    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
    };

    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(LiftProjectFromAgg::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[test]
    fn lift_exprs_global_agg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![
                    unresolved_col("a").sum(),
                    unresolved_col("a")
                        .sum()
                        .add(unresolved_col("b").sum())
                        .alias("a_plus_b"),
                    unresolved_col("b").mean(),
                    unresolved_col("b").mean().alias("c"),
                ],
                vec![],
            )?
            .build();

        let schema = dummy_scan_node(scan_op.clone()).schema();

        let a_sum_id = unresolved_col("a").sum().semantic_id(schema.as_ref()).id;
        let b_sum_id = unresolved_col("b").sum().semantic_id(schema.as_ref()).id;
        let b_mean_id = unresolved_col("b").mean().semantic_id(schema.as_ref()).id;

        let expected = dummy_scan_node(scan_op)
            .aggregate(
                vec![
                    unresolved_col("a").sum().alias(a_sum_id.clone()),
                    unresolved_col("b").sum().alias(b_sum_id.clone()),
                    unresolved_col("b").mean().alias(b_mean_id.clone()),
                ],
                vec![],
            )?
            .select(vec![
                unresolved_col(a_sum_id.clone()).alias("a"),
                unresolved_col(a_sum_id)
                    .add(unresolved_col(b_sum_id))
                    .alias("a_plus_b"),
                unresolved_col(b_mean_id.clone()).alias("b"),
                unresolved_col(b_mean_id).alias("c"),
            ])?
            .build();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn lift_exprs_groupby_agg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("groupby_key", DataType::Utf8),
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![
                    unresolved_col("a").sum(),
                    unresolved_col("a")
                        .sum()
                        .add(unresolved_col("b").sum())
                        .alias("a_plus_b"),
                    unresolved_col("b").mean(),
                    unresolved_col("b").mean().alias("c"),
                ],
                vec![unresolved_col("groupby_key")],
            )?
            .build();

        let schema = dummy_scan_node(scan_op.clone()).schema();

        let a_sum_id = unresolved_col("a").sum().semantic_id(schema.as_ref()).id;
        let b_sum_id = unresolved_col("b").sum().semantic_id(schema.as_ref()).id;
        let b_mean_id = unresolved_col("b").mean().semantic_id(schema.as_ref()).id;

        let expected = dummy_scan_node(scan_op)
            .aggregate(
                vec![
                    unresolved_col("a").sum().alias(a_sum_id.clone()),
                    unresolved_col("b").sum().alias(b_sum_id.clone()),
                    unresolved_col("b").mean().alias(b_mean_id.clone()),
                ],
                vec![unresolved_col("groupby_key")],
            )?
            .select(vec![
                unresolved_col("groupby_key"),
                unresolved_col(a_sum_id.clone()).alias("a"),
                unresolved_col(a_sum_id)
                    .add(unresolved_col(b_sum_id))
                    .alias("a_plus_b"),
                unresolved_col(b_mean_id.clone()).alias("b"),
                unresolved_col(b_mean_id).alias("c"),
            ])?
            .build();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn do_not_lift_exprs_global_agg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![
                    unresolved_col("a").sum(),
                    unresolved_col("a")
                        .add(unresolved_col("b"))
                        .sum()
                        .alias("a_plus_b"),
                    unresolved_col("b").mean(),
                    unresolved_col("b").mean().alias("c"),
                ],
                vec![],
            )?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn do_not_lift_exprs_groupby_agg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("groupby_key", DataType::Utf8),
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![
                    unresolved_col("a").sum(),
                    unresolved_col("a")
                        .add(unresolved_col("b"))
                        .sum()
                        .alias("a_plus_b"),
                    unresolved_col("b").mean(),
                    unresolved_col("b").mean().alias("c"),
                ],
                vec![unresolved_col("groupby_key")],
            )?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
