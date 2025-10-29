use std::{collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_treenode::{DynTreeNode, Transformed, TreeNode};
use daft_algebra::boolean::{combine_conjunction, split_conjunction};
use daft_core::{join::JoinType, prelude::SchemaRef};
use daft_dsl::{Column, Expr, ExprRef, Operator, ResolvedColumn, Subquery, resolved_col};
use itertools::multiunzip;
use uuid::Uuid;

use super::OptimizerRule;
use crate::{
    LogicalPlan, LogicalPlanRef,
    logical_plan::downcast_subquery,
    ops::{Aggregate, Filter, Join, Project, join::JoinPredicate},
};

/// Rewriter rule to convert scalar subqueries into joins.
///
/// ## Examples
/// ### Example 1 - Uncorrelated subquery
/// Before:
/// ```sql
/// SELECT val
/// FROM tbl1
/// WHERE key = (SELECT max(key) FROM tbl2)
/// ```
/// After:
/// ```sql
/// SELECT val
/// FROM tbl1
/// CROSS JOIN (SELECT max(key) FROM tbl2) AS subquery
/// WHERE key = subquery.key  -- this can be then pushed into join in a future rule
/// ```
///
/// ### Example 2 - Correlated subquery
/// Before:
/// ```sql
/// SELECT val
/// FROM tbl1
/// WHERE outer_key =
///     (
///         SELECT max(outer_key)
///         FROM tbl2
///         WHERE inner_key = tbl1.inner_key
///     )
/// ```
/// After:
/// ```sql
/// SELECT val
/// FROM tbl1
/// LEFT JOIN
///     (
///         SELECT inner_key, max(outer_key)
///         FROM tbl2
///         GROUP BY inner_key
///     ) AS subquery
/// ON inner_key
/// WHERE outer_key = subquery.outer_key
/// ```
#[derive(Debug)]
pub struct UnnestScalarSubquery {}

impl UnnestScalarSubquery {
    pub fn new() -> Self {
        Self {}
    }
}

impl UnnestScalarSubquery {
    fn unnest_subqueries(
        input: LogicalPlanRef,
        exprs: &[ExprRef],
    ) -> DaftResult<Transformed<(LogicalPlanRef, Vec<ExprRef>)>> {
        let mut subqueries = HashSet::new();

        let new_exprs = exprs
            .iter()
            .map(|expr| {
                expr.clone()
                    .transform_down(|e| {
                        if let Expr::Subquery(subquery) = e.as_ref() {
                            subqueries.insert(subquery.clone());

                            Ok(Transformed::yes(resolved_col(subquery.semantic_id().id)))
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })
                    .unwrap()
                    .data
            })
            .collect();

        if subqueries.is_empty() {
            return Ok(Transformed::no((input, new_exprs)));
        }

        let new_input = subqueries
            .into_iter()
            .try_fold(input, |curr_input, subquery| {
                let subquery_alias = subquery.semantic_id().id;
                let subquery_plan = downcast_subquery(&subquery);

                let subquery_col_names = subquery_plan.schema().names();
                let [output_col] = subquery_col_names.as_slice() else {
                    return Err(DaftError::ValueError(format!(
                        "Expected scalar subquery to have one output column, received: {}",
                        subquery_col_names.len()
                    )));
                };

                // alias output column
                let subquery_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                    subquery_plan,
                    vec![resolved_col(output_col.as_str()).alias(subquery_alias)],
                )?));

                let (decorrelated_subquery, subquery_on, input_on) =
                    pull_up_correlated_cols(subquery_plan)?;

                let on_expr =
                    combine_conjunction(input_on.into_iter().zip(subquery_on.into_iter()).map(
                        |(i, s)| {
                            let i_left = i
                                .to_left_cols(curr_input.schema())
                                .expect("input columns to be in curr_input");
                            let s_right = s
                                .to_right_cols(decorrelated_subquery.schema())
                                .expect("subquery columns to be in decorrelated_subquery");

                            i_left.eq(s_right)
                        },
                    ));

                // use inner join when uncorrelated so that filter can be pushed into join and other optimizations
                let join_type = if on_expr.is_none() {
                    JoinType::Inner
                } else {
                    JoinType::Left
                };

                let (curr_input, decorrelated_subquery, on_expr) = Join::deduplicate_join_columns(
                    curr_input,
                    decorrelated_subquery,
                    on_expr,
                    &[],
                    join_type,
                    Default::default(),
                )?;

                let on = JoinPredicate::try_new(on_expr)?;

                Ok(Arc::new(LogicalPlan::Join(Join::try_new(
                    curr_input,
                    decorrelated_subquery,
                    on,
                    join_type,
                    None,
                )?)))
            })?;

        Ok(Transformed::yes((new_input, new_exprs)))
    }
}

impl OptimizerRule for UnnestScalarSubquery {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| match node.as_ref() {
            LogicalPlan::Filter(Filter {
                input, predicate, ..
            }) => {
                let unnest_result =
                    Self::unnest_subqueries(input.clone(), &split_conjunction(predicate))?;

                if !unnest_result.transformed {
                    return Ok(Transformed::no(node));
                }

                let (new_input, new_predicates) = unnest_result.data;

                let new_predicate = combine_conjunction(new_predicates)
                    .expect("predicates are guaranteed to exist at this point, so 'conjunct' should never return 'None'");

                let new_filter = Arc::new(LogicalPlan::Filter(Filter::try_new(
                    new_input,
                    new_predicate,
                )?));

                // preserve original schema
                let new_plan = Arc::new(LogicalPlan::Project(Project::new_from_schema(
                    new_filter,
                    input.schema(),
                )?));

                Ok(Transformed::yes(new_plan))
            }
            LogicalPlan::Project(Project {
                input, projection, ..
            }) => {
                let unnest_result =
                    Self::unnest_subqueries(input.clone(), projection)?;

                if !unnest_result.transformed {
                    return Ok(Transformed::no(node));
                }

                let (new_input, new_projection) = unnest_result.data;

                // preserve original schema
                let new_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                    new_input,
                    new_projection,
                )?));

                Ok(Transformed::yes(new_plan))
            }
            _ => Ok(Transformed::no(node)),
        })
    }
}

/// Rewriter rule to convert IN and EXISTS subqueries into joins.
///
/// ## Examples
/// ### Example 1 - Uncorrelated `IN` Query
/// Before:
/// ```sql
/// SELECT val
/// FROM tbl1
/// WHERE key IN (SELECT key FROM tbl2)
/// ```
/// After:
/// ```sql
/// SELECT val
/// FROM tbl1
/// SEMI JOIN (SELECT key FROM tbl2) AS subquery
/// ON key = subquery.key
/// ```
///
/// ### Example 2 - Correlated `NOT EXISTS` Query
/// Before:
/// ```sql
/// SELECT val
/// FROM tbl1
/// WHERE NOT EXISTS
///     (
///         SELECT *
///         FROM tbl2
///         WHERE key = tbl1.key
///     )
/// ```
///
/// After:
/// ```sql
/// SELECT val
/// FROM tbl1
/// ANTI JOIN (SELECT * FROM tbl2) AS subquery
/// ON key = subquery.key
/// ```
#[derive(Debug)]
pub struct UnnestPredicateSubquery {}

impl UnnestPredicateSubquery {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Eq, Hash, PartialEq)]
struct PredicateSubquery {
    pub subquery: Subquery,
    pub in_expr: Option<ExprRef>,
    pub join_type: JoinType,
}

impl OptimizerRule for UnnestPredicateSubquery {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| match node.as_ref() {
            LogicalPlan::Filter(Filter {
                input, predicate, ..
            }) => {
                let mut subqueries = HashSet::new();

                let new_predicates = split_conjunction(predicate)
                    .into_iter()
                    .filter(|expr| {
                        match expr.as_ref() {
                            Expr::InSubquery(in_expr, subquery) => {
                                subqueries.insert(PredicateSubquery { subquery: subquery.clone(), in_expr: Some(in_expr.clone()), join_type: JoinType::Semi });
                                false
                            }
                            Expr::Exists(subquery) => {
                                subqueries.insert(PredicateSubquery { subquery: subquery.clone(), in_expr: None, join_type: JoinType::Semi });
                                false
                            }
                            Expr::Not(e) => {
                                match e.as_ref() {
                                    Expr::InSubquery(in_expr, subquery) => {
                                        subqueries.insert(PredicateSubquery { subquery: subquery.clone(), in_expr: Some(in_expr.clone()), join_type: JoinType::Anti });
                                        false
                                    }
                                    Expr::Exists(subquery) => {
                                        subqueries.insert(PredicateSubquery { subquery: subquery.clone(), in_expr: None, join_type: JoinType::Anti });
                                        false
                                    }
                                    _ => true
                                }
                            }
                            _ => true
                        }
                    })
                    .collect::<Vec<_>>();

                if subqueries.is_empty() {
                    return Ok(Transformed::no(node));
                }

                let new_input = subqueries.into_iter().try_fold(input.clone(), |curr_input, PredicateSubquery { subquery, in_expr, join_type }| {
                    let subquery_plan = downcast_subquery(&subquery);
                    let subquery_schema = subquery_plan.schema();

                    let (decorrelated_subquery, mut subquery_on, mut input_on) =
                        pull_up_correlated_cols(subquery_plan)?;

                    if let Some(in_expr) = in_expr {
                        let subquery_col_names = subquery_schema.names();
                        let [output_col] = subquery_col_names.as_slice() else {
                            return Err(DaftError::ValueError(format!("Expected IN subquery to have one output column, received: {}", subquery_col_names.len())));
                        };

                        input_on.push(in_expr);
                        subquery_on.push(resolved_col(output_col.as_str()));
                    }

                    if subquery_on.is_empty() {
                        return Err(DaftError::ValueError("Expected IN/EXISTS subquery to be correlated, found uncorrelated subquery.".to_string()));
                    }

                    let on_expr = combine_conjunction(input_on.into_iter().zip(subquery_on.into_iter()).map(
                        |(i, s)| {
                            let i_left = i
                                .to_left_cols(curr_input.schema())
                                .expect("input columns to be in curr_input");
                            let s_right = s
                                .to_right_cols(decorrelated_subquery.schema())
                                .expect("subquery columns to be in decorrelated_subquery");

                            i_left.eq(s_right)
                        },
                    ));

                    let on = JoinPredicate::try_new(on_expr)?;

                    Ok(Arc::new(LogicalPlan::Join(Join::try_new(
                        curr_input,
                        decorrelated_subquery,
                        on,
                        join_type,
                        None,
                    )?)))
                })?;

                let new_plan = if let Some(new_predicate) = combine_conjunction(new_predicates) {
                    // add filter back if there are non-subquery predicates
                    Arc::new(LogicalPlan::Filter(Filter::try_new(
                        new_input,
                        new_predicate,
                    )?))
                } else {
                    new_input
                };

                Ok(Transformed::yes(new_plan))
            }
            _ => Ok(Transformed::no(node)),
        })
    }
}

fn pull_up_correlated_cols(
    plan: LogicalPlanRef,
) -> DaftResult<(LogicalPlanRef, Vec<ExprRef>, Vec<ExprRef>)> {
    let (new_inputs, subquery_on, outer_on): (Vec<_>, Vec<_>, Vec<_>) = multiunzip(
        plan.arc_children()
            .into_iter()
            .map(pull_up_correlated_cols)
            .collect::<DaftResult<Vec<_>>>()?,
    );

    let plan = if new_inputs.is_empty() {
        plan
    } else {
        Arc::new(plan.with_new_children(&new_inputs))
    };

    let mut subquery_on = subquery_on.into_iter().flatten().collect::<Vec<_>>();
    let mut outer_on = outer_on.into_iter().flatten().collect::<Vec<_>>();

    match plan.as_ref() {
        LogicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let mut found_correlated_col = false;

            let preds = split_conjunction(predicate)
                .into_iter()
                .filter(|expr| {
                    if let Expr::BinaryOp {
                        op: Operator::Eq,
                        left,
                        right,
                    } = expr.as_ref()
                    {
                        match (left.as_ref(), right.as_ref()) {
                            (
                                Expr::Column(Column::Resolved(ResolvedColumn::Basic(
                                    subquery_col_name,
                                ))),
                                Expr::Column(Column::Resolved(ResolvedColumn::OuterRef(
                                    outer_field,
                                    _,
                                ))),
                            )
                            | (
                                Expr::Column(Column::Resolved(ResolvedColumn::OuterRef(
                                    outer_field,
                                    _,
                                ))),
                                Expr::Column(Column::Resolved(ResolvedColumn::Basic(
                                    subquery_col_name,
                                ))),
                            ) => {
                                // remove correlated col from filter, use in join instead
                                subquery_on.push(resolved_col(subquery_col_name.clone()));
                                outer_on.push(resolved_col(outer_field.name.as_str()));

                                found_correlated_col = true;
                                return false;
                            }
                            _ => {}
                        }
                    }

                    true
                })
                .collect::<Vec<_>>();

            // no new correlated cols found
            if !found_correlated_col {
                return Ok((plan.clone(), subquery_on, outer_on));
            }

            if let Some(new_predicate) = combine_conjunction(preds) {
                let new_plan = Arc::new(LogicalPlan::Filter(Filter::try_new(
                    input.clone(),
                    new_predicate,
                )?));

                Ok((new_plan, subquery_on, outer_on))
            } else {
                // all predicates are correlated so filter can be completely removed
                Ok((input.clone(), subquery_on, outer_on))
            }
        }
        LogicalPlan::Project(Project {
            input,
            projection,
            projected_schema,
            ..
        }) => {
            // ensure all columns that need to be pulled up are in the projection

            let (new_subquery_on, missing_exprs) =
                get_missing_exprs(subquery_on, projection, projected_schema);

            if missing_exprs.is_empty() {
                // project already contains all necessary columns
                Ok((plan.clone(), new_subquery_on, outer_on))
            } else {
                let new_projection = [projection.clone(), missing_exprs].concat();

                let new_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                    input.clone(),
                    new_projection,
                )?));

                Ok((new_plan, new_subquery_on, outer_on))
            }
        }
        LogicalPlan::Aggregate(Aggregate {
            input,
            aggregations,
            groupby,
            output_schema,
            ..
        }) => {
            // put columns that need to be pulled up into the groupby

            let (new_subquery_on, missing_groupbys) =
                get_missing_exprs(subquery_on, groupby, output_schema);

            if missing_groupbys.is_empty() {
                // agg already contains all necessary columns
                Ok((plan.clone(), new_subquery_on, outer_on))
            } else {
                let new_groupby = [groupby.clone(), missing_groupbys].concat();

                let new_plan = Arc::new(LogicalPlan::Aggregate(Aggregate::try_new(
                    input.clone(),
                    aggregations.clone(),
                    new_groupby,
                )?));

                Ok((new_plan, new_subquery_on, outer_on))
            }
        }

        // ops that can trivially pull up correlated cols
        LogicalPlan::Distinct(..)
        | LogicalPlan::MonotonicallyIncreasingId(..)
        | LogicalPlan::Repartition(..)
        | LogicalPlan::IntoBatches(..)
        | LogicalPlan::Union(..)
        | LogicalPlan::Intersect(..)
        | LogicalPlan::Sort(..)
        | LogicalPlan::SubqueryAlias(..) => Ok((plan.clone(), subquery_on, outer_on)),

        // ops that cannot pull up correlated columns
        LogicalPlan::UDFProject(..)
        | LogicalPlan::Limit(..)
        | LogicalPlan::Offset(..)
        | LogicalPlan::Shard(..)
        | LogicalPlan::TopN(..)
        | LogicalPlan::Sample(..)
        | LogicalPlan::Source(..)
        | LogicalPlan::Explode(..)
        | LogicalPlan::Unpivot(..)
        | LogicalPlan::Pivot(..)
        | LogicalPlan::Concat(..)
        | LogicalPlan::Join(..)
        | LogicalPlan::Sink(..)
        | LogicalPlan::Window(..)
        | LogicalPlan::VLLMProject(..) => {
            if subquery_on.is_empty() {
                Ok((plan.clone(), vec![], vec![]))
            } else {
                Err(DaftError::NotImplemented(format!(
                    "Pulling up correlated columns not supported for: {}",
                    plan.name()
                )))
            }
        }
    }
}

fn get_missing_exprs(
    subquery_on: Vec<ExprRef>,
    existing_exprs: &[ExprRef],
    schema: &SchemaRef,
) -> (Vec<ExprRef>, Vec<ExprRef>) {
    let mut new_subquery_on = Vec::new();
    let mut missing_exprs = Vec::new();

    for expr in subquery_on {
        if existing_exprs.contains(&expr) {
            // column already exists in schema
            new_subquery_on.push(expr);
        } else if schema.has_field(expr.name()) {
            // another expression takes pull up column name, we rename the pull up column.
            let new_name = format!("{}-{}", expr.name(), Uuid::new_v4());

            new_subquery_on.push(resolved_col(new_name.clone()));
            missing_exprs.push(expr.alias(new_name));
        } else {
            // missing from schema, can keep original name

            new_subquery_on.push(expr.clone());
            missing_exprs.push(expr);
        }
    }

    (new_subquery_on, missing_exprs)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::join::JoinType;
    use daft_dsl::{Column, Expr, PlanRef, ResolvedColumn, Subquery, unresolved_col};
    use daft_schema::{dtype::DataType, field::Field};

    use super::{UnnestPredicateSubquery, UnnestScalarSubquery};
    use crate::{
        LogicalPlanRef,
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
    };

    fn assert_scalar_optimized_plan_eq(
        plan: LogicalPlanRef,
        expected: LogicalPlanRef,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(UnnestScalarSubquery::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    fn assert_predicate_optimized_plan_eq(
        plan: LogicalPlanRef,
        expected: LogicalPlanRef,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(UnnestPredicateSubquery::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[test]
    fn uncorrelated_scalar_subquery() -> DaftResult<()> {
        let tbl1 = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("key", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]));

        let tbl2 = dummy_scan_node(dummy_scan_operator(vec![Field::new(
            "key",
            DataType::Int64,
        )]));

        let subquery = tbl2.aggregate(vec![unresolved_col("key").max()], vec![])?;
        let subquery_expr = Arc::new(Expr::Subquery(Subquery {
            plan: subquery.build(),
        }));
        let subquery_alias = subquery_expr.semantic_id(&subquery.schema()).id;

        let plan = tbl1
            .filter(unresolved_col("key").eq(subquery_expr))?
            .select(vec![unresolved_col("val")])?
            .build();

        let expected = tbl1
            .join(
                subquery.select(vec![unresolved_col("key").alias(subquery_alias.clone())])?,
                None,
                vec![],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .filter(unresolved_col("key").eq(unresolved_col(subquery_alias)))?
            .select(vec![unresolved_col("key"), unresolved_col("val")])?
            .select(vec![unresolved_col("val")])?
            .build();

        assert_scalar_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn correlated_scalar_subquery() -> DaftResult<()> {
        let tbl1 = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("outer_key", DataType::Int64),
            Field::new("inner_key", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]));

        let tbl2 = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("outer_key", DataType::Int64),
            Field::new("inner_key2", DataType::Int64),
        ]));

        let subquery =
            tbl2.filter(unresolved_col("inner_key2").eq(Arc::new(Expr::Column(
                Column::Resolved(ResolvedColumn::OuterRef(
                    Field::new("inner_key", DataType::Int64),
                    PlanRef::Unqualified,
                )),
            ))))?
            .aggregate(vec![unresolved_col("outer_key").max()], vec![])?;
        let subquery_expr = Arc::new(Expr::Subquery(Subquery {
            plan: subquery.build(),
        }));
        let subquery_alias = subquery_expr.semantic_id(&subquery.schema()).id;

        let plan = tbl1
            .filter(unresolved_col("outer_key").eq(subquery_expr))?
            .select(vec![unresolved_col("val")])?
            .build();

        let expected = tbl1
            .join(
                tbl2.aggregate(
                    vec![unresolved_col("outer_key").max()],
                    vec![unresolved_col("inner_key2")],
                )?
                .select(vec![
                    unresolved_col("outer_key").alias(subquery_alias.clone()),
                    unresolved_col("inner_key2"),
                ])?,
                unresolved_col("inner_key")
                    .eq(unresolved_col("inner_key2"))
                    .into(),
                vec![],
                JoinType::Left,
                None,
                Default::default(),
            )?
            .filter(unresolved_col("outer_key").eq(unresolved_col(subquery_alias)))?
            .select(vec![
                unresolved_col("outer_key"),
                unresolved_col("inner_key"),
                unresolved_col("val"),
            ])?
            .select(vec![unresolved_col("val")])?
            .build();

        assert_scalar_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn uncorrelated_predicate_subquery() -> DaftResult<()> {
        let tbl1 = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("key", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]));

        let tbl2 = dummy_scan_node(dummy_scan_operator(vec![Field::new(
            "key2",
            DataType::Int64,
        )]));

        let plan = tbl1
            .filter(Arc::new(Expr::InSubquery(
                unresolved_col("key"),
                Subquery { plan: tbl2.build() },
            )))?
            .select(vec![unresolved_col("val")])?
            .build();

        let expected = tbl1
            .join(
                tbl2,
                unresolved_col("key").eq(unresolved_col("key2")).into(),
                vec![],
                JoinType::Semi,
                None,
                Default::default(),
            )?
            .select(vec![unresolved_col("val")])?
            .build();

        assert_predicate_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn correlated_predicate_subquery() -> DaftResult<()> {
        let tbl1 = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("key", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]));

        let tbl2 = dummy_scan_node(dummy_scan_operator(vec![Field::new(
            "key2",
            DataType::Int64,
        )]));

        let subquery = tbl2
            .filter(
                unresolved_col("key2").eq(Arc::new(Expr::Column(Column::Resolved(
                    ResolvedColumn::OuterRef(
                        Field::new("key", DataType::Int64),
                        PlanRef::Unqualified,
                    ),
                )))),
            )?
            .build();

        let plan = tbl1
            .filter(Arc::new(Expr::Exists(Subquery { plan: subquery })).not())?
            .select(vec![unresolved_col("val")])?
            .build();

        let expected = tbl1
            .join(
                tbl2,
                unresolved_col("key").eq(unresolved_col("key2")).into(),
                vec![],
                JoinType::Anti,
                None,
                Default::default(),
            )?
            .select(vec![unresolved_col("val")])?
            .build();

        assert_predicate_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
