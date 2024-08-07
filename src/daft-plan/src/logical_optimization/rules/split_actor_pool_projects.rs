use std::{collections::HashMap, default, sync::Arc};

use common_error::DaftResult;
use common_treenode::{TreeNode, TreeNodeRewriter};
use daft_dsl::{
    functions::{
        python::{PythonUDF, StatefulPythonUDF},
        FunctionExpr,
    },
    optimization::requires_computation,
    Expr, ExprRef,
};

use crate::{
    logical_ops::{ActorPoolProject, Project},
    LogicalPlan, ResourceRequest,
};

use super::{ApplyOrder, OptimizerRule, Transformed};

#[derive(Default, Debug)]
pub struct SplitActorPoolProjects {}

/// Implement SplitActorPoolProjects as an OptimizerRule which will:
///
/// 1. Go top-down from the root of the LogicalPlan
/// 2. Whenever it sees a Project, it will iteratively split it into the necessary Project/ActorPoolProject sequences,
///     depending on the underlying Expressions' layouts of StatefulPythonUDFs.
///
/// The general idea behind the splitting is that this is a greedy algorithm which will:
/// * Skim off the top of every expression in the projection to generate "stages" for every expression
/// * Generate Project/ActorPoolProject nodes based on those stages (coalesce non-stateful stages into a Project, and run the stateful stages as ActorPoolProjects sequentially)
/// * Loop until every expression in the projection has been exhausted
///
/// For a given expression tree, skimming a Stage off the top entails:
/// 1. Iterate down the root of the tree, stopping whenever we encounter a StatefulUDF expression
/// 2. If the current stage is rooted at a StatefulUDF expression, then replace its children with Expr::Columns and return the StatefulUDF expression as its own stage
/// 3. Otherwise, the current stage is not a StatefulUDF expression: chop off any StatefulUDF children and replace them with Expr::Columns
impl OptimizerRule for SplitActorPoolProjects {
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::TopDown
    }

    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // TODO: Figure out num_actors! How do we propagate this correctly?
        let num_actors = 1;

        match plan.as_ref() {
            LogicalPlan::Project(projection) => try_optimize_project(
                projection,
                plan.clone(),
                &projection.resource_request,
                num_actors,
            ),
            // TODO: Figure out how to split other nodes as well such as Filter, Agg etc
            _ => Ok(Transformed::No(plan)),
        }
    }
}

struct SplitExprByStatefulUDF {
    // Initialized to True, but once we encounter non-aliases this will be set to false
    is_parsing_stateful_udf: bool,
    next_exprs: Vec<ExprRef>,
}

impl SplitExprByStatefulUDF {
    fn new() -> Self {
        Self {
            is_parsing_stateful_udf: true,
            next_exprs: Vec::new(),
        }
    }
}

impl TreeNodeRewriter for SplitExprByStatefulUDF {
    type Node = ExprRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            // Encountered alias: keep going if we are ignoring aliases
            Expr::Alias { .. } if self.is_parsing_stateful_udf => {
                Ok(common_treenode::Transformed::no(node))
            }
            // Encountered stateful UDF: chop off all children and add to self.next_exprs
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF { .. })),
                inputs,
            } => {
                assert!(self.is_parsing_stateful_udf, "SplitExprByStatefulUDF.is_parsing_stateful_udf should be True if we encounter a stateful UDF expression");

                let new_inputs = inputs.iter().map(|e| {
                    if requires_computation(e.as_ref()) {
                        // Truncate the child if it requires computation, and push it onto the stack to indicate that it needs computation in a different stage
                        self.next_exprs.push(e.clone());
                        Expr::Column(e.name().into()).arced()
                    } else {
                        e.clone()
                    }
                });
                let new_truncated_node = node.with_new_children(new_inputs.collect()).arced();

                Ok(common_treenode::Transformed::new(
                    new_truncated_node,
                    true,
                    common_treenode::TreeNodeRecursion::Jump,
                ))
            }
            expr => {
                // Indicate that we are now parsing a stateless expression tree
                self.is_parsing_stateful_udf = false;

                // None of the direct children are stateful UDFs, so we keep going
                if node.children().iter().all(|e| {
                    !matches!(
                        e.as_ref(),
                        Expr::Function {
                            func: FunctionExpr::Python(PythonUDF::Stateful(
                                StatefulPythonUDF { .. }
                            )),
                            ..
                        }
                    )
                }) {
                    return Ok(common_treenode::Transformed::no(node));
                }

                // If any children are stateful UDFs, we truncate
                let inputs = expr.children();
                let new_inputs = inputs.iter().map(|e| {
                    if matches!(
                        e.as_ref(),
                        Expr::Function {
                            func: FunctionExpr::Python(PythonUDF::Stateful(
                                StatefulPythonUDF { .. }
                            )),
                            ..
                        }
                    ) {
                        self.next_exprs.push(e.clone());
                        Expr::Column(e.name().into()).arced()
                    } else {
                        e.clone()
                    }
                });
                let new_truncated_node = node.with_new_children(new_inputs.collect()).arced();

                Ok(common_treenode::Transformed::yes(new_truncated_node))
            }
        }
    }
}

fn try_optimize_project(
    projection: &Project,
    plan: Arc<LogicalPlan>,
    stateful_resource_request: &ResourceRequest,
    num_actors: usize,
) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
    // Simple common case: no stateful UDFs at all and we have no transformations
    let has_stateful_udfs = projection.projection.iter().any(has_stateful_udf);
    if !has_stateful_udfs {
        return Ok(Transformed::No(plan));
    }

    let final_column_names = projection
        .projection
        .iter()
        .map(|e| e.name())
        .collect::<Vec<_>>();
    let (remaining, next_stages): (Vec<ExprRef>, Vec<ExprRef>) = {
        let mut remaining = Vec::new();
        let mut next_stages = Vec::new();
        for expr in projection.projection.iter() {
            let mut rewriter = SplitExprByStatefulUDF::new();
            let root = expr.clone().rewrite(&mut rewriter)?.data;
            next_stages.push(root);
            remaining.extend(rewriter.next_exprs);
        }
        (remaining, next_stages)
    };

    // Start building the tree back up starting from the children
    let new_plan_child = if remaining.is_empty() {
        // Nothing remaining, we're done splitting and should wire the new node up with the child of the Project
        plan.children()[0].clone()
    } else {
        // Recursively run the rule on the new child Project
        let new_project = Project::try_new(
            plan.children()[0].clone(),
            remaining,
            stateful_resource_request.clone(),
        )?;
        let new_child_project = LogicalPlan::Project(new_project.clone()).arced();
        let optimized_child_plan = try_optimize_project(
            &new_project,
            new_child_project.clone(),
            stateful_resource_request,
            num_actors,
        )?;
        optimized_child_plan.unwrap().clone()
    };

    // Conditionally build on the tree next with a stateless Project
    let stateless_stages: HashMap<&str, &ExprRef> = next_stages
        .iter()
        .filter(|&e| !has_stateful_udf(e))
        .map(|stateless_expr| (stateless_expr.name(), stateless_expr))
        .collect();
    let new_plan = if stateless_stages.is_empty() {
        new_plan_child.clone()
    } else {
        // NOTE: We set the resource request to default here because we want to avoid inheriting the resource request which is likely
        // intended for the stateful UDF only (e.g. GPUs). We should figure out a better way to do this, perhaps having it on the Expression-level?
        let stateless_projection = final_column_names
            .iter()
            .map(|&name| {
                if let Some(&stateless_expr) = stateless_stages.get(name) {
                    stateless_expr.clone()
                } else {
                    Expr::Column(name.into()).arced()
                }
            })
            .collect();
        let stateless_projection_resource_request = default::Default::default();
        LogicalPlan::Project(Project::try_new(
            new_plan_child.clone(),
            stateless_projection,
            stateless_projection_resource_request,
        )?)
        .arced()
    };

    // Build on the tree with the necessary stateful ActorPoolProject nodes
    let new_plan = {
        let mut child = new_plan;
        for stateful_expr in next_stages.iter().filter(|&e| has_stateful_udf(e)) {
            let stateful_expr_name = stateful_expr.name();
            let stateful_projection = final_column_names
                .iter()
                .map(|&name| {
                    if name == stateful_expr_name {
                        stateful_expr.clone()
                    } else {
                        Expr::Column(name.into()).arced()
                    }
                })
                .collect();
            child = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
                child,
                stateful_projection,
                stateful_resource_request.clone(),
                num_actors,
            )?)
            .arced();
        }
        child
    };

    Ok(Transformed::Yes(new_plan))
}

#[inline]
fn has_stateful_udf(e: &ExprRef) -> bool {
    e.exists(|e| {
        matches!(
            e.as_ref(),
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(_)),
                ..
            }
        )
    })
}
