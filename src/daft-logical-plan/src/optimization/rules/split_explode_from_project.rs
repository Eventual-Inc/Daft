use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{resolved_col, Expr};

use crate::{
    ops::{Explode, Project},
    optimization::rules::OptimizerRule,
    LogicalPlan, LogicalPlanRef,
};

#[derive(Debug)]
pub struct SplitExplodeFromProject {}

impl SplitExplodeFromProject {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SplitExplodeFromProject {
    fn try_optimize(&self, plan: LogicalPlanRef) -> DaftResult<Transformed<LogicalPlanRef>> {
        plan.transform_down(|node| {
            let LogicalPlan::Project(projection) = node.as_ref() else {
                return Ok(Transformed::no(node));
            };

            let input_schema = projection.input.schema();

            let mut to_explode = Vec::new();
            let new_projection = projection
                .projection
                .iter()
                .map(|expr| {
                    let name = expr.get_name(&input_schema)?;
                    let mut unaliased_expr = expr;
                    while let Expr::Alias(inner, _) = unaliased_expr.as_ref() {
                        unaliased_expr = inner;
                    }

                    if let Expr::ScalarFunction(sf) = unaliased_expr.as_ref()
                        && sf.is_function_type::<daft_functions_list::Explode>()
                    {
                        let inner = sf
                            .inputs
                            .first()
                            .expect("explode should have exactly one input argument");

                        let inner = if inner.get_name(&input_schema)? != name {
                            inner.alias(name.clone())
                        } else {
                            inner.clone()
                        };

                        to_explode.push(resolved_col(name));

                        Ok(inner)
                    } else {
                        Ok(expr.clone())
                    }
                })
                .collect::<DaftResult<Vec<_>>>()?;

            if to_explode.is_empty() {
                Ok(Transformed::no(node))
            } else {
                Ok(Transformed::yes(
                    Explode::try_new(
                        Project::try_new(projection.input.clone(), new_projection)?.into(),
                        to_explode,
                    )?
                    .into(),
                ))
            }
        })
    }
}
