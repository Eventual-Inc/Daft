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
            LogicalPlan::Explode(_) => {
                // TODO
                Ok(None)
            }
            LogicalPlan::Sort(..)
            | LogicalPlan::Repartition(..)
            | LogicalPlan::Coalesce(..)
            | LogicalPlan::Limit(..)
            | LogicalPlan::Filter(..) => {
                // Get required columns from projection and upstream.
                let combined_dependencies = upstream_plan
                    .required_columns()
                    .iter()
                    .flatten()
                    .chain(plan.required_columns().iter().flatten())
                    .cloned()
                    .collect::<HashSet<_>>();

                // Skip projection if no columns would be pruned.
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
            LogicalPlan::Distinct(_) => {
                // TODO
                Ok(None)
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
            LogicalPlan::Concat(_) => {
                // TODO
                Ok(None)
            }
            LogicalPlan::Join(_) => {
                // TODO
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

// get upstream output schema
// get projection contents
//  - if exact columns, can drop

// get upstream unchanged columns
//  - fold projection

// get upstream required columns
//  -

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
