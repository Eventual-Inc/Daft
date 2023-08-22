use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;

use crate::{ops::Project, LogicalPlan};

use super::{ApplyOrder, OptimizerRule};

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
            LogicalPlan::Project(_child_project) => {
                // TODO
                None
            }
            LogicalPlan::Explode(_) => {
                // TODO
                None
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

                    let pushdown_schema: SchemaRef = {
                        let fields = pushdown_column_exprs
                            .iter()
                            .map(|e| e.to_field(&upstream_schema).unwrap())
                            .collect::<Vec<_>>();
                        Schema::new(fields).unwrap().into()
                    };

                    Project::new(
                        pushdown_column_exprs,
                        pushdown_schema,
                        Default::default(),
                        grand_upstream_plan.clone(),
                    )
                    .into()
                };

                let new_sort = upstream_plan.with_new_children(&[new_subprojection.into()]);
                Some(plan.with_new_children(&[new_sort]))
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
