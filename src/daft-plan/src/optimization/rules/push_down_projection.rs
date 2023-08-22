use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    thread::LocalKey,
};

use common_error::DaftResult;
use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{
    col,
    optimization::{get_required_columns, replace_columns_with_expressions},
    Expr,
};

use crate::{
    ops::{Concat, Filter, Project, Sort},
    optimization::rules::push_down_filter,
    LogicalPlan,
};

use super::{
    utils::{conjuct, split_conjuction},
    ApplyOrder, OptimizerRule,
};

#[derive(Default)]
pub struct PushDownProjection {}

impl PushDownProjection {
    pub fn new() -> Self {
        Self {}
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
        let projection = match plan {
            LogicalPlan::Project(project) => project,
            _ => return Ok(None),
        };
        let upstream_plan = projection.input.as_ref();
        let upstream_schema = upstream_plan.schema();

        let new_plan = match upstream_plan {
            LogicalPlan::Source(_) => {
                // TODO
                None
            }
            LogicalPlan::Project(child_project) => {
                // TODO
                None
            }
            LogicalPlan::Filter(_) => {
                // TODO
                None
            }
            LogicalPlan::Limit(_) => {
                // TODO
                None
            }
            LogicalPlan::Explode(_) => {
                // TODO
                None
            }
            LogicalPlan::Sort(sort) => {
                // Get required columns from projection and upstream.
                let combined_dependencies = sort
                    .sort_by
                    .iter()
                    .map(|e| get_required_columns(e))
                    .chain(
                        projection
                            .projection
                            .iter()
                            .map(|e| get_required_columns(e)),
                    )
                    .flatten()
                    .collect::<HashSet<_>>();

                let pushdown_column_exprs = combined_dependencies
                    .into_iter()
                    .map(|s| Expr::Column(s.into()))
                    .collect::<Vec<_>>();

                let pushdown_schema: SchemaRef = {
                    let fields = pushdown_column_exprs
                        .iter()
                        .map(|e| e.to_field(&upstream_schema).unwrap())
                        .collect::<Vec<_>>();
                    Schema::new(fields).unwrap().into()
                };

                let new_subprojection: LogicalPlan = Project::new(
                    pushdown_column_exprs,
                    pushdown_schema,
                    Default::default(),
                    sort.input.clone(),
                )
                .into();

                let new_sort = upstream_plan.with_new_children(&[new_subprojection.into()]);
                Some(plan.with_new_children(&[new_sort]))
            }
            LogicalPlan::Repartition(_) => {
                // TODO
                None
            }
            LogicalPlan::Coalesce(_) => {
                // TODO
                None
            }
            LogicalPlan::Distinct(_) => {
                // TODO
                None
            }
            LogicalPlan::Aggregate(_) => {
                // TODO
                None
            }
            LogicalPlan::Concat(_) => {
                // TODO
                None
            }
            LogicalPlan::Join(_) => {
                // TODO
                None
            }
            LogicalPlan::Sink(_) => unreachable!(),
        };
        Ok(new_plan)
    }
}
