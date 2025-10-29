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
#[derive(Default, Debug)]
pub struct SplitVLLM;

impl OptimizerRule for SplitVLLM {
    fn try_optimize(&self, plan: LogicalPlanRef) -> DaftResult<Transformed<LogicalPlanRef>> {
        plan.transform(|node| {
            let LogicalPlan::Project(project) = node.as_ref() else {
                return Ok(Transformed::no(node));
            };

            let mut input_expr_and_name = None;

            let replaced_projection = project
                .projection
                .iter()
                .map(|expr| {
                    expr.clone()
                        .transform(|e| {
                            if let Expr::VLLM(vllm_expr) = e.as_ref() {
                                let id = e.semantic_id(&project.input.schema()).id;
                                input_expr_and_name = Some((vllm_expr.clone(), id.clone()));
                                Ok(Transformed::yes(resolved_col(id)))
                            } else {
                                Ok(Transformed::no(e))
                            }
                        })
                        .map(|t| t.data)
                })
                .collect::<DaftResult<Vec<_>>>()?;

            let Some((vllm_expr, output_column_name)) = input_expr_and_name else {
                return Ok(Transformed::no(node));
            };

            let vllm_project = LogicalPlan::VLLMProject(VLLMProject::new(
                project.input.clone(),
                vllm_expr,
                output_column_name,
            ))
            .arced();
            let final_project =
                LogicalPlan::Project(Project::new(vllm_project, replaced_projection)?).arced();

            Ok(Transformed::yes(final_project))
        })
    }
}
