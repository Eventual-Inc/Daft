use std::sync::Once;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{Expr, ExprRef};
use daft_onnx::{OnnxPlanner, load_onnx_bytes, ops::register_onnx_ops, parse_model};

use crate::{LogicalPlan, LogicalPlanRef, optimization::rules::OptimizerRule};

static INIT: Once = Once::new();

fn ensure_onnx_ops_registered() {
    INIT.call_once(register_onnx_ops);
}

/// Resolves `Expr::OnnxModel` into the actual expression tree(s) parsed from the .onnx file.
#[derive(Default, Debug)]
pub struct ResolveOnnxModel;

impl OptimizerRule for ResolveOnnxModel {
    fn try_optimize(&self, plan: LogicalPlanRef) -> DaftResult<Transformed<LogicalPlanRef>> {
        ensure_onnx_ops_registered();

        plan.transform(|node| {
            let LogicalPlan::Project(project) = node.as_ref() else {
                return Ok(Transformed::no(node));
            };

            let mut any_changed = false;
            let mut new_projection = Vec::new();

            for expr in &project.projection {
                // Recursively resolve any OnnxModel exprs deep in the tree.
                let resolved = expr.clone().transform(|e| resolve_single_output(e))?;
                if resolved.transformed {
                    any_changed = true;
                }
                let expr = resolved.data;

                // Check if the top-level is a (possibly aliased) OnnxModel
                // that might expand into multiple outputs.
                match try_expand_multi_output(&expr)? {
                    Some(exprs) => {
                        any_changed = true;
                        new_projection.extend(exprs);
                    }
                    None => new_projection.push(expr),
                }
            }

            if !any_changed {
                return Ok(Transformed::no(node));
            }

            let new_project = LogicalPlan::Project(crate::ops::Project::new(
                project.input.clone(),
                new_projection,
            )?)
            .arced();
            Ok(Transformed::yes(new_project))
        })
    }
}

/// Resolve a single `Expr::OnnxModel` into its first output expression.
fn resolve_single_output(e: ExprRef) -> DaftResult<Transformed<ExprRef>> {
    let Expr::OnnxModel(onnx_expr) = e.as_ref() else {
        return Ok(Transformed::no(e));
    };

    let mut exprs = resolve_onnx_model(onnx_expr)?;
    Ok(Transformed::yes(exprs.remove(0)))
}

/// If the top-level expr (possibly under Alias) is an OnnxModel, resolve and
/// return all output expressions.
fn try_expand_multi_output(expr: &ExprRef) -> DaftResult<Option<Vec<ExprRef>>> {
    match expr.as_ref() {
        Expr::OnnxModel(onnx_expr) => Ok(Some(resolve_onnx_model(onnx_expr)?)),
        Expr::Alias(inner, _) if matches!(inner.as_ref(), Expr::OnnxModel(_)) => {
            if let Expr::OnnxModel(onnx_expr) = inner.as_ref() {
                Ok(Some(resolve_onnx_model(onnx_expr)?))
            } else {
                unreachable!()
            }
        }
        _ => Ok(None),
    }
}

fn resolve_onnx_model(
    onnx_expr: &daft_dsl::expr::OnnxModelExpr,
) -> DaftResult<Vec<ExprRef>> {
    let bytes = load_onnx_bytes(&onnx_expr.model, onnx_expr.io_config.as_ref()).map_err(|e| {
        common_error::DaftError::ValueError(format!("Failed to load ONNX model: {e}"))
    })?;
    let model = parse_model(&bytes).map_err(|e| {
        common_error::DaftError::ValueError(format!("Failed to parse ONNX model: {e}"))
    })?;

    let planner = OnnxPlanner::new();
    planner.plan(&model, &onnx_expr.inputs).map_err(|e| {
        common_error::DaftError::ValueError(format!("Failed to plan ONNX model: {e}"))
    })
}
