use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_dsl::{
    expr::{bound_col, BoundColumn},
    Column, Expr, ExprRef,
};

use crate::{ops::Project, optimization::rules::OptimizerRule, LogicalPlan, LogicalPlanRef};

trait SplitExprSeries {
    fn should_split(&self, expr: &Expr) -> DaftResult<bool>;
    fn expr_to_node(
        &self,
        expr: ExprRef,
        input: LogicalPlanRef,
    ) -> DaftResult<(LogicalPlanRef, Vec<usize>)>;
}

impl<T: SplitExprSeries> OptimizerRule for T {
    fn try_optimize(&self, plan: LogicalPlanRef) -> DaftResult<Transformed<LogicalPlanRef>> {
        plan.transform_up(|node| {
            let LogicalPlan::Project(Project {
                input, projection, ..
            }) = node.as_ref()
            else {
                return Ok(Transformed::no(node));
            };

            let input_schema = input.schema();

            let mut split_expr = None;
            let mut split_expr_children = vec![];

            let final_exprs = projection
                .iter()
                .map(|expr| {
                    // We only get one expression to split, so simply propagate the rest
                    // They will be split in the later recursive call to `try_optimize`
                    if split_expr.is_some() {
                        return Ok(expr.clone());
                    }

                    expr.clone()
                        .transform_up(|e| {
                            if !self.should_split(&e)? {
                                return Ok(Transformed::no(e));
                            }

                            split_expr_children.clone_from(&e.children());

                            let new_children = e
                                .children()
                                .iter()
                                .enumerate()
                                .map(|(i, child)| {
                                    let child_index = input_schema.len() + i;
                                    let child_field = child.to_field(&input_schema)?;

                                    Ok(bound_col(child_index, child_field))
                                })
                                .collect::<DaftResult<Vec<_>>>()?;

                            split_expr = Some(Arc::new(e.with_new_children(new_children)));

                            let index = input_schema.len();
                            let field = e.to_field(&input_schema)?;

                            Ok(Transformed {
                                data: bound_col(index, field),
                                transformed: true,
                                tnr: TreeNodeRecursion::Stop,
                            })
                        })
                        .map(|t| t.data)
                })
                .collect::<DaftResult<Vec<_>>>()?;

            let Some(split_expr) = split_expr else {
                return Ok(Transformed::no(node));
            };

            let first_exprs = input_schema
                .into_iter()
                .enumerate()
                .map(|(i, f)| bound_col(i, f.clone()))
                .chain(split_expr_children)
                .collect::<Vec<_>>();

            let first_proj = Project::try_new(input.clone(), first_exprs)?.into();

            let (split_node, index_mapping) = self.expr_to_node(split_expr, first_proj)?;

            debug_assert_eq!(index_mapping.len(), input_schema.len() + 1);

            let split_node_schema = split_node.schema();

            let final_exprs = final_exprs
                .into_iter()
                .map(|expr| {
                    expr.transform(|e| {
                        if let Expr::Column(Column::Bound(BoundColumn { index, .. })) = e.as_ref() {
                            let new_index = index_mapping[*index];
                            let field = split_node_schema[new_index].clone();

                            Ok(Transformed::yes(bound_col(new_index, field)))
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })
                    .map(|t| t.data)
                })
                .collect::<DaftResult<Vec<_>>>()?;

            let final_proj = Project::try_new(split_node, final_exprs)?.into();

            // Recursive call to split the other expressions as well
            let recursed = self.try_optimize(final_proj)?.data;
            Ok(Transformed::yes(recursed))
        })
    }
}
