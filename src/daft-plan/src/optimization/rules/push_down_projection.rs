use std::{collections::HashSet, convert::identity, sync::Arc};

use common_error::DaftResult;

use daft_dsl::Expr;

use crate::{
    ops::{Aggregate, Project},
    LogicalPlan,
};

use super::{ApplyOrder, OptimizerRule};

#[derive(Default)]
pub struct PushDownProjection {}

impl PushDownProjection {
    pub fn new() -> Self {
        Self {}
    }

    fn try_optimize_project(
        &self,
        projection: &Project,
        plan: &LogicalPlan,
    ) -> DaftResult<Option<Arc<LogicalPlan>>> {
        let upstream_plan = projection.input.clone();
        let upstream_schema = upstream_plan.schema();

        // First, drop this projection if it is a no-op.
        let projection_is_noop = {
            let maybe_column_names = projection
                .projection
                .iter()
                .map(|e| match e {
                    Expr::Column(colname) => Some(colname.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>();

            let all_columns_equal = maybe_column_names.map_or(false, |column_names| {
                column_names
                    .iter()
                    .zip(upstream_schema.names().iter())
                    .map(|(scol, pcol)| scol.as_ref() == pcol)
                    .all(identity)
            });

            let no_unused_columns = upstream_schema.names().len() == projection.projection.len();

            all_columns_equal && no_unused_columns
        };
        if projection_is_noop {
            // Projection discarded but new root node has not been looked at;
            // look at the new root node.
            let new_plan = self.try_optimize(&upstream_plan)?.unwrap_or(upstream_plan);
            return Ok(Some(new_plan));
        }

        match upstream_plan.as_ref() {
            LogicalPlan::Source(_source) => {
                // TODO
                Ok(None)
            }
            LogicalPlan::Project(upstream_projection) => {
                // Prune unnecessary columns from the child projection.
                let required_columns = &plan.required_columns()[0];
                if required_columns.len() < upstream_schema.names().len() {
                    let pruned_upstream_projections = upstream_projection
                        .projection
                        .iter()
                        .filter_map(|e| {
                            required_columns
                                .contains(e.name().unwrap())
                                .then(|| e.clone())
                        })
                        .collect::<Vec<_>>();

                    let new_upstream: LogicalPlan = Project::try_new(
                        upstream_projection.input.clone(),
                        pruned_upstream_projections,
                        upstream_projection.resource_request.clone(),
                    )?
                    .into();

                    let new_plan = plan.with_new_children(&[new_upstream.into()]);
                    // Retry optimization now that the upstream node is different.
                    let new_plan = self.try_optimize(&new_plan)?.unwrap_or(new_plan);
                    Ok(Some(new_plan))
                } else {
                    Ok(None)
                }
            }
            LogicalPlan::Aggregate(aggregate) => {
                // Prune unnecessary columns from the child aggregate.
                let required_columns = &plan.required_columns()[0];
                let pruned_aggregate_exprs = aggregate
                    .aggregations
                    .iter()
                    .filter_map(|e| {
                        required_columns
                            .contains(e.name().unwrap())
                            .then(|| e.clone())
                    })
                    .collect::<Vec<_>>();

                if pruned_aggregate_exprs.len() < aggregate.aggregations.len() {
                    let new_upstream: LogicalPlan = Aggregate::try_new(
                        aggregate.input.clone(),
                        pruned_aggregate_exprs,
                        aggregate.groupby.clone(),
                    )?
                    .into();

                    let new_plan = plan.with_new_children(&[new_upstream.into()]);
                    // Retry optimization now that the upstream node is different.
                    let new_plan = self.try_optimize(&new_plan)?.unwrap_or(new_plan);
                    Ok(Some(new_plan))
                } else {
                    Ok(None)
                }
            }
            LogicalPlan::Sort(..)
            | LogicalPlan::Repartition(..)
            | LogicalPlan::Coalesce(..)
            | LogicalPlan::Limit(..)
            | LogicalPlan::Filter(..)
            | LogicalPlan::Explode(..) => {
                // Get required columns from projection and upstream.
                let combined_dependencies = upstream_plan
                    .required_columns()
                    .iter()
                    .flatten()
                    .chain(plan.required_columns().iter().flatten())
                    .cloned()
                    .collect::<HashSet<_>>();

                // Skip optimization if no columns would be pruned.
                let grand_upstream_plan = upstream_plan.children()[0];
                let grand_upstream_columns = grand_upstream_plan.schema().names();
                if grand_upstream_columns.len() == combined_dependencies.len() {
                    return Ok(None);
                }

                let new_subprojection: LogicalPlan = {
                    let pushdown_column_exprs = combined_dependencies
                        .into_iter()
                        .map(|s| Expr::Column(s.into()))
                        .collect::<Vec<_>>();

                    Project::try_new(
                        grand_upstream_plan.clone(),
                        pushdown_column_exprs,
                        Default::default(),
                    )?
                    .into()
                };

                let new_upstream = upstream_plan.with_new_children(&[new_subprojection.into()]);
                let new_plan = plan.with_new_children(&[new_upstream]);
                // Retry optimization now that the upstream node is different.
                let new_plan = self.try_optimize(&new_plan)?.unwrap_or(new_plan);
                Ok(Some(new_plan))
            }
            LogicalPlan::Concat(concat) => {
                // Get required columns from projection and upstream.
                let combined_dependencies = upstream_plan
                    .required_columns()
                    .iter()
                    .flatten()
                    .chain(plan.required_columns().iter().flatten())
                    .cloned()
                    .collect::<HashSet<_>>();

                // Skip optimization if no columns would be pruned.
                let grand_upstream_plan = upstream_plan.children()[0];
                let grand_upstream_columns = grand_upstream_plan.schema().names();
                if grand_upstream_columns.len() == combined_dependencies.len() {
                    return Ok(None);
                }

                let pushdown_column_exprs = combined_dependencies
                    .into_iter()
                    .map(|s| Expr::Column(s.into()))
                    .collect::<Vec<_>>();
                let new_left_subprojection: LogicalPlan = {
                    Project::try_new(
                        concat.input.clone(),
                        pushdown_column_exprs.clone(),
                        Default::default(),
                    )?
                    .into()
                };
                let new_right_subprojection: LogicalPlan = {
                    Project::try_new(
                        concat.other.clone(),
                        pushdown_column_exprs.clone(),
                        Default::default(),
                    )?
                    .into()
                };

                let new_upstream = upstream_plan.with_new_children(&[
                    new_left_subprojection.into(),
                    new_right_subprojection.into(),
                ]);
                let new_plan = plan.with_new_children(&[new_upstream]);
                // Retry optimization now that the upstream node is different.
                let new_plan = self.try_optimize(&new_plan)?.unwrap_or(new_plan);
                Ok(Some(new_plan))
            }
            LogicalPlan::Join(join) => {
                // Get required columns from projection and both upstreams.
                let [projection_required_columns] = &plan.required_columns()[..] else {
                    panic!()
                };
                let [left_dependencies, right_dependencies] = &upstream_plan.required_columns()[..]
                else {
                    panic!()
                };

                let left_upstream_names = join
                    .input
                    .schema()
                    .names()
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>();
                let right_upstream_names = join
                    .right
                    .schema()
                    .names()
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>();

                let right_combined_dependencies = projection_required_columns
                    .iter()
                    .filter_map(|colname| join.right_input_mapping.get(colname))
                    .chain(right_dependencies.iter())
                    .cloned()
                    .collect::<HashSet<_>>();

                let left_combined_dependencies = projection_required_columns
                    .iter()
                    .filter_map(|colname| left_upstream_names.get(colname))
                    .chain(left_dependencies.iter())
                    // We also have to keep any name conflict columns referenced by the right side.
                    // E.g. if the user wants "right.c", left must also provide "c", or "right.c" disappears.
                    // This is mostly an artifact of https://github.com/Eventual-Inc/Daft/issues/1303
                    .chain(
                        right_combined_dependencies
                            .iter()
                            .filter_map(|rname| left_upstream_names.get(rname)),
                    )
                    .cloned()
                    .collect::<HashSet<_>>();

                // For each upstream, see if a non-vacuous pushdown is possible.
                let maybe_new_left_upstream: Option<Arc<LogicalPlan>> = {
                    if left_combined_dependencies.len() < left_upstream_names.len() {
                        let pushdown_column_exprs = left_combined_dependencies
                            .into_iter()
                            .map(|s| Expr::Column(s.into()))
                            .collect::<Vec<_>>();
                        let new_project: LogicalPlan = Project::try_new(
                            join.input.clone(),
                            pushdown_column_exprs,
                            Default::default(),
                        )?
                        .into();
                        Some(new_project.into())
                    } else {
                        None
                    }
                };

                let maybe_new_right_upstream: Option<Arc<LogicalPlan>> = {
                    if right_combined_dependencies.len() < right_upstream_names.len() {
                        let pushdown_column_exprs = right_combined_dependencies
                            .into_iter()
                            .map(|s| Expr::Column(s.into()))
                            .collect::<Vec<_>>();
                        let new_project: LogicalPlan = Project::try_new(
                            join.right.clone(),
                            pushdown_column_exprs,
                            Default::default(),
                        )?
                        .into();
                        Some(new_project.into())
                    } else {
                        None
                    }
                };

                // If either pushdown is possible, create a new Join node.
                if maybe_new_left_upstream
                    .as_ref()
                    .or(maybe_new_right_upstream.as_ref())
                    .is_some()
                {
                    let new_left_upstream = maybe_new_left_upstream.unwrap_or(join.input.clone());
                    let new_right_upstream = maybe_new_right_upstream.unwrap_or(join.right.clone());
                    let new_join =
                        upstream_plan.with_new_children(&[new_left_upstream, new_right_upstream]);
                    let new_plan = plan.with_new_children(&[new_join]);
                    // Retry optimization now that the upstream node is different.
                    let new_plan = self.try_optimize(&new_plan)?.unwrap_or(new_plan);
                    Ok(Some(new_plan))
                } else {
                    Ok(None)
                }
            }
            LogicalPlan::Distinct(_) => {
                // Cannot push down past a Distinct,
                // since Distinct implicitly requires all parent columns.
                Ok(None)
            }
            LogicalPlan::Sink(_) => {
                panic!("Bad projection due to upstream sink node: {:?}", projection)
            }
        }
    }

    fn try_optimize_aggregation(
        &self,
        aggregation: &Aggregate,
        plan: &LogicalPlan,
    ) -> DaftResult<Option<Arc<LogicalPlan>>> {
        // If this aggregation prunes columns from its upstream,
        // then explicitly create a projection to do so.
        let upstream_plan = &aggregation.input;
        let upstream_schema = upstream_plan.schema();

        let aggregation_required_cols = &plan.required_columns()[0];
        if aggregation_required_cols.len() < upstream_schema.names().len() {
            let new_subprojection: LogicalPlan = {
                let pushdown_column_exprs = aggregation_required_cols
                    .iter()
                    .map(|s| Expr::Column(s.clone().into()))
                    .collect::<Vec<_>>();

                Project::try_new(
                    upstream_plan.clone(),
                    pushdown_column_exprs,
                    Default::default(),
                )?
                .into()
            };

            let new_aggregation = plan.with_new_children(&[new_subprojection.into()]);
            Ok(Some(new_aggregation))
        } else {
            Ok(None)
        }
    }
}

impl OptimizerRule for PushDownProjection {
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn try_optimize(&self, plan: &LogicalPlan) -> DaftResult<Option<Arc<LogicalPlan>>> {
        match plan {
            LogicalPlan::Project(projection) => self.try_optimize_project(projection, plan),
            // Aggregations also do column projection
            LogicalPlan::Aggregate(aggregation) => self.try_optimize_aggregation(aggregation, plan),
            _ => Ok(None),
        }
    }
}
