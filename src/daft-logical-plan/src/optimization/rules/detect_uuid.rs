use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{Column, Expr, ExprRef, ResolvedColumn, functions::scalar::ScalarFn};

use crate::{
    logical_plan::{LogicalPlan, Project},
    ops::Uuid,
    optimization::rules::OptimizerRule,
};

#[derive(Debug)]
pub struct DetectUuid;

impl Default for DetectUuid {
    fn default() -> Self {
        Self
    }
}

impl DetectUuid {
    pub fn new() -> Self {
        Self
    }

    fn is_uuid_expr(expr: &ExprRef) -> bool {
        match expr.as_ref() {
            Expr::ScalarFn(ScalarFn::Builtin(func)) => func.name() == "uuid",
            _ => expr.children().iter().any(Self::is_uuid_expr),
        }
    }

    fn contains_uuid(project: &Project) -> bool {
        project.projection.iter().any(Self::is_uuid_expr)
    }
}

impl OptimizerRule for DetectUuid {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| match node.as_ref() {
            LogicalPlan::Project(project) => {
                if Self::contains_uuid(project) {
                    let generated_column_names = std::cell::RefCell::new(Vec::<Arc<str>>::new());

                    let new_projection: DaftResult<Vec<ExprRef>> = project
                        .projection
                        .iter()
                        .map(|expr| {
                            Ok(expr
                                .clone()
                                .transform(|e| match e.as_ref() {
                                    Expr::ScalarFn(ScalarFn::Builtin(func))
                                        if func.name() == "uuid" =>
                                    {
                                        let column_name: Arc<str> =
                                            Arc::from(format!("uuid-{}", uuid::Uuid::new_v4()));
                                        generated_column_names
                                            .borrow_mut()
                                            .push(column_name.clone());
                                        Ok(Transformed::yes(
                                            Expr::Column(Column::Resolved(ResolvedColumn::Basic(
                                                column_name,
                                            )))
                                            .into(),
                                        ))
                                    }
                                    _ => Ok(Transformed::no(e)),
                                })?
                                .data)
                        })
                        .collect();
                    let new_projection = new_projection?;

                    let mut uuid_input = project.input.clone();
                    for column_name in generated_column_names.into_inner() {
                        uuid_input = Arc::new(LogicalPlan::Uuid(Uuid::try_new(
                            uuid_input,
                            Some(column_name.as_ref()),
                        )?));
                    }

                    let final_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                        uuid_input,
                        new_projection,
                    )?));

                    Ok(Transformed::yes(final_plan))
                } else {
                    Ok(Transformed::no(node))
                }
            }
            _ => Ok(Transformed::no(node)),
        })
    }
}
