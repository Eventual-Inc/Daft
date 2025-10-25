use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{Expr, resolved_col};

use crate::{
    LogicalPlan, LogicalPlanRef,
    ops::{Project, VLLMProject},
    optimization::rules::OptimizerRule,
};

/// Split VLLM expressions out of projections into separate VLLMProject nodes.
///
/// Current limitations:
/// - Only supports one VLLM expression per project
/// - Not sure how this interacts with other optimization rules, especially SplitUDFs
/// - Requires that no input columns are named "daft_vllm_output"
#[derive(Default, Debug)]
pub struct SplitVLLM;

impl OptimizerRule for SplitVLLM {
    fn try_optimize(&self, plan: LogicalPlanRef) -> DaftResult<Transformed<LogicalPlanRef>> {
        plan.transform(|node| {
            let LogicalPlan::Project(project) = node.as_ref() else {
                return Ok(Transformed::no(node));
            };

            let mut input_col = None;

            let replaced_projection = project
                .projection
                .iter()
                .map(|expr| {
                    expr.clone()
                        .transform(|e| {
                            if matches!(e.as_ref(), Expr::VLLM(..)) {
                                input_col = Some(e);
                                Ok(Transformed::yes(resolved_col("daft_vllm_output")))
                            } else {
                                Ok(Transformed::no(e))
                            }
                        })
                        .map(|t| t.data)
                })
                .collect::<DaftResult<Vec<_>>>()?;

            let Some(Expr::VLLM(vllm_expr)) = input_col.as_deref() else {
                return Ok(Transformed::no(node));
            };

            let vllm_project = LogicalPlan::VLLMProject(VLLMProject::new(
                project.input.clone(),
                vllm_expr.clone(),
            ))
            .arced();
            let final_project =
                LogicalPlan::Project(Project::new(vllm_project, replaced_projection)?).arced();

            Ok(Transformed::yes(final_project))
        })
    }
}
