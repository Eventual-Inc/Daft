use std::{collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_treenode::{DynTreeNode, Transformed, TreeNode};
use daft_core::{join::JoinType, prelude::SchemaRef};
use daft_dsl::{
    col,
    optimization::{conjuct, split_conjuction},
    Expr, ExprRef, Operator, OuterReferenceColumn, Subquery,
};
use daft_schema::field::Field;
use itertools::multiunzip;
use uuid::Uuid;

use super::OptimizerRule;
use crate::{
    logical_plan::downcast_subquery,
    ops::{Aggregate, Filter, Join, Project},
    LogicalPlan, LogicalPlanRef,
};

/// Rewriter rule to convert scalar subqueries into joins.
#[derive(Debug)]
pub struct EliminateScalarSubquery {}

impl EliminateScalarSubquery {
    pub fn new() -> Self {
        Self {}
    }
}

impl EliminateScalarSubquery {
    fn unnest_subqueries(
        input: LogicalPlanRef,
        exprs: Vec<&ExprRef>,
    ) -> DaftResult<Transformed<(LogicalPlanRef, Vec<ExprRef>)>> {
        let mut subqueries = HashSet::new();

        let new_exprs = exprs
            .into_iter()
            .map(|expr| {
                expr.clone()
                    .transform_down(|e| {
                        if let Expr::Subquery(subquery) = e.as_ref() {
                            subqueries.insert(subquery.clone());

                            Ok(Transformed::yes(col(subquery.semantic_id().id)))
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
                    vec![col(output_col.as_str()).alias(subquery_alias)],
                )?));

                let (decorrelated_subquery, subquery_on, input_on) =
                    pull_up_correlated_cols(subquery_plan)?;

                if subquery_on.is_empty() {
                    // uncorrelated scalar subquery
                    Ok(Arc::new(LogicalPlan::Join(Join::try_new(
                        curr_input,
                        decorrelated_subquery,
                        vec![],
                        vec![],
                        None,
                        JoinType::Inner,
                        None,
                        None,
                        None,
                        false,
                    )?)))
                } else {
                    // correlated scalar subquery
                    Ok(Arc::new(LogicalPlan::Join(Join::try_new(
                        curr_input,
                        decorrelated_subquery,
                        input_on,
                        subquery_on,
                        None,
                        JoinType::Left,
                        None,
                        None,
                        None,
                        false,
                    )?)))
                }
            })?;

        Ok(Transformed::yes((new_input, new_exprs)))
    }
}

impl OptimizerRule for EliminateScalarSubquery {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| match node.as_ref() {
            LogicalPlan::Filter(Filter {
                input, predicate, ..
            }) => {
                let unnest_result =
                    Self::unnest_subqueries(input.clone(), split_conjuction(predicate))?;

                if !unnest_result.transformed {
                    return Ok(Transformed::no(node));
                }

                let (new_input, new_predicates) = unnest_result.data;

                let new_predicate = conjuct(new_predicates)
                    .expect("predicates should exist in unnested subquery filter");

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
                    Self::unnest_subqueries(input.clone(), projection.iter().collect())?;

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
#[derive(Debug)]
pub struct EliminatePredicateSubquery {}

impl EliminatePredicateSubquery {
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

impl OptimizerRule for EliminatePredicateSubquery {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| match node.as_ref() {
            LogicalPlan::Filter(Filter {
                input, predicate, ..
            }) => {
                let mut subqueries = HashSet::new();

                let new_predicates = split_conjuction(predicate)
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
                    .cloned()
                    .collect::<Vec<_>>();

                if subqueries.is_empty() {
                    return Ok(Transformed::no(node));
                }

                let new_input = subqueries.into_iter().try_fold(input.clone(), |curr_input, PredicateSubquery { subquery, in_expr, join_type }| {
                    let subquery_plan = downcast_subquery(&subquery);

                    let (decorrelated_subquery, mut subquery_on, mut input_on) =
                        pull_up_correlated_cols(subquery_plan.clone())?;

                    if let Some(in_expr) = in_expr {
                        let subquery_col_names = subquery_plan.schema().names();
                        let [output_col] = subquery_col_names.as_slice() else {
                            return Err(DaftError::ValueError(format!("Expected IN subquery to have one output column, received: {}", subquery_col_names.len())));
                        };

                        input_on.push(in_expr);
                        subquery_on.push(col(output_col.as_str()));
                    }

                    if subquery_on.is_empty() {
                        return Err(DaftError::ValueError("Expected IN/EXISTS subquery to be correlated, found uncorrelated subquery.".to_string()));
                    }

                    Ok(Arc::new(LogicalPlan::Join(Join::try_new(
                        curr_input,
                        decorrelated_subquery,
                        input_on,
                        subquery_on,
                        None,
                        join_type,
                        None,
                        None,
                        None,
                        false
                    )?)))
                })?;

                let new_plan = if let Some(new_predicate) = conjuct(new_predicates) {
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

            let preds = split_conjuction(predicate)
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
                                Expr::Column(subquery_col),
                                Expr::OuterReferenceColumn(OuterReferenceColumn {
                                    field:
                                        Field {
                                            name: outer_col, ..
                                        },
                                    ..
                                }),
                            )
                            | (
                                Expr::OuterReferenceColumn(OuterReferenceColumn {
                                    field:
                                        Field {
                                            name: outer_col, ..
                                        },
                                    ..
                                }),
                                Expr::Column(subquery_col),
                            ) => {
                                // remove correlated col from filter, use in join instead
                                subquery_on.push(col(subquery_col.clone()));
                                outer_on.push(col(outer_col.as_str()));

                                found_correlated_col = true;
                                return false;
                            }
                            _ => {}
                        }
                    }

                    true
                })
                .cloned()
                .collect::<Vec<_>>();

            // no new correlated cols found
            if !found_correlated_col {
                return Ok((plan.clone(), subquery_on, outer_on));
            }

            if let Some(new_predicate) = conjuct(preds) {
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
        | LogicalPlan::Limit(..)
        | LogicalPlan::MonotonicallyIncreasingId(..)
        | LogicalPlan::Repartition(..)
        | LogicalPlan::Sample(..)
        | LogicalPlan::Union(..)
        | LogicalPlan::Intersect(..)
        | LogicalPlan::Sort(..) => Ok((plan.clone(), subquery_on, outer_on)),

        // ops that cannot pull up correlated columns
        LogicalPlan::ActorPoolProject(..)
        | LogicalPlan::Source(..)
        | LogicalPlan::Explode(..)
        | LogicalPlan::Unpivot(..)
        | LogicalPlan::Pivot(..)
        | LogicalPlan::Concat(..)
        | LogicalPlan::Join(..)
        | LogicalPlan::Sink(..) => {
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
    let (new_subquery_on, missing_exprs): (Vec<_>, Vec<_>) = subquery_on
        .into_iter()
        .map(|expr| {
            if existing_exprs.contains(&expr) {
                // column already exists in schema
                (expr, None)
            } else if schema.has_field(expr.name()) {
                // another expression takes pull up column name, we rename the pull up column.
                let unique_id = Uuid::new_v4().to_string();
                let aliased_col = expr.alias(format!("{}_{}", expr.name(), unique_id));
                (col(unique_id), Some(aliased_col))
            } else {
                (expr.clone(), Some(expr))
            }
        })
        .unzip();

    let missing_exprs = missing_exprs.into_iter().flatten().collect();

    (new_subquery_on, missing_exprs)
}
