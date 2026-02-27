use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{Expr, functions::scalar::ScalarFn, resolved_col};

use crate::{
    LogicalPlan, LogicalPlanRef,
    ops::{Explode, Project},
    optimization::rules::OptimizerRule,
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

            let mut ignore_empty_and_null: Option<bool> = None;
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

                    if let Expr::ScalarFn(ScalarFn::Builtin(sf)) = unaliased_expr.as_ref()
                        && sf.is_function_type::<daft_functions_list::Explode>()
                    {
                        let current_ignore_empty_and_null = if sf.inputs.len() == 2 {
                            match sf
                                .inputs
                                .iter()
                                .nth(1)
                                .unwrap()
                                .inner()
                                .as_literal()
                                .and_then(|l| l.as_bool())
                            {
                                Some(b) => b,
                                None => return Ok(expr.clone()), // Cannot optimize non-literal ignore_empty_and_null
                            }
                        } else {
                            false
                        };

                        if let Some(d) = ignore_empty_and_null {
                            if d != current_ignore_empty_and_null {
                                // Conflicting ignore_empty_and_null flags, cannot optimize into single Explode
                                return Ok(expr.clone());
                            }
                        } else {
                            ignore_empty_and_null = Some(current_ignore_empty_and_null);
                        }

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
                // Explode columns with inconsistent `ignore_empty_and_null` flags are skipped (kept as scalar
                // functions in the projection). Only columns with consistent `ignore_empty_and_null` are
                // extracted into the Explode operator.
                let ignore_empty_and_null = ignore_empty_and_null.unwrap_or(false);

                Ok(Transformed::yes(
                    Explode::try_new(
                        Project::try_new(projection.input.clone(), new_projection)?.into(),
                        to_explode,
                        ignore_empty_and_null,
                        None,
                    )?
                    .into(),
                ))
            }
        })
    }
}
