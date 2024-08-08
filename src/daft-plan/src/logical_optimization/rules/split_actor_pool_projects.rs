use std::{collections::HashSet, default, iter, sync::Arc};

use common_error::DaftResult;
use common_treenode::{TreeNode, TreeNodeRewriter};
use daft_dsl::{
    functions::{
        python::{PythonUDF, StatefulPythonUDF},
        FunctionExpr,
    },
    optimization::{get_required_columns, requires_computation},
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

    // Start building a chain of `child -> Optional<Project> -> ActorPoolProject -> ActorPoolProject -> ...`
    let (stateful_stages, stateless_stages): (Vec<_>, Vec<_>) =
        next_stages.into_iter().partition(has_stateful_udf);
    let stateless_stages_names: HashSet<String> = stateless_stages
        .iter()
        .map(|e| e.name().to_string())
        .collect();

    // Conditionally build on the tree next with a stateless Project
    let new_plan = if stateless_stages.is_empty() {
        new_plan_child.clone()
    } else {
        // Stateless projection consists of stateless expressions, but also pass-through of any columns required by subsequent stateful ActorPoolProjects
        let stateful_stages_columns_required: HashSet<String> = stateful_stages
            .iter()
            .flat_map(get_required_columns)
            .collect();
        let stateless_projection = stateless_stages
            .into_iter()
            .chain(stateful_stages_columns_required.iter().filter_map(|name| {
                if stateless_stages_names.contains(name.as_str()) {
                    None
                } else {
                    Some(Expr::Column(name.as_str().into()).arced())
                }
            }))
            .collect();

        // NOTE: We set the resource request to default here because we want to avoid inheriting the resource request which is likely
        // intended for the stateful UDF only (e.g. GPUs). We should figure out a better way to do this, perhaps having it on the Expression-level?
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

        for idx in 0..stateful_stages.len() {
            let stateful_expr = stateful_stages[idx].clone();
            let stateful_expr_name = stateful_expr.name().to_string();
            let remaining_stateful_stages_columns_required: HashSet<String> = stateful_stages
                .as_slice()[idx + 1..]
                .iter()
                .flat_map(get_required_columns)
                .collect();
            let stateful_projection = remaining_stateful_stages_columns_required
                .iter()
                .chain(stateless_stages_names.iter())
                .filter_map(|name| {
                    if name == &stateful_expr_name {
                        None
                    } else {
                        Some(Expr::Column(name.as_str().into()).arced())
                    }
                })
                .chain(iter::once(stateful_expr))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::datatypes::Field;
    use daft_dsl::{
        col,
        functions::{
            python::{PythonUDF, StatefulPythonUDF},
            FunctionExpr,
        },
        Expr, ExprRef,
    };

    use crate::{
        logical_ops::{ActorPoolProject, Project},
        logical_optimization::test::assert_optimized_plan_with_rules_eq,
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan, ResourceRequest,
    };

    use super::SplitActorPoolProjects;

    /// Helper that creates an optimizer with the SplitExprByStatefulUDF rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![Box::new(SplitActorPoolProjects {})],
        )
    }

    fn create_stateful_udf(inputs: Vec<ExprRef>) -> ExprRef {
        Expr::Function {
            func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                name: Arc::new("foo".to_string()),
                num_expressions: 1,
                return_dtype: daft_core::DataType::Binary,
            })),
            inputs,
        }
        .arced()
    }

    fn create_resource_request() -> ResourceRequest {
        crate::ResourceRequest {
            num_cpus: Some(8.),
            num_gpus: Some(1.),
            memory_bytes: None,
        }
    }

    // TODO: need to figure out how users will pass this in
    static NUM_ACTORS: usize = 1;

    #[test]
    fn test_with_column_stateful_udf_happypath() -> DaftResult<()> {
        let resource_request = create_resource_request();
        let scan_op = dummy_scan_operator(vec![Field::new("a", daft_core::DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stateful_project_expr = create_stateful_udf(vec![col("a")]);

        // Add a Projection with StatefulUDF and resource request
        let project_plan = scan_plan
            .with_columns(
                vec![stateful_project_expr.clone().alias("b")],
                resource_request.clone(),
            )?
            .build();

        // Project([col("a")]) --> ActorPoolProject([col("a"), foo(col("a")).alias("b")])
        let expected = scan_plan.select(vec![col("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![col("a"), stateful_project_expr.clone().alias("b")],
            // Actor pool project has the specified resource request, but the normal Project has the default resource request
            resource_request.clone(),
            NUM_ACTORS,
        )?)
        .arced();

        assert_optimized_plan_eq(project_plan, expected)?;

        Ok(())
    }

    #[test]
    fn test_multiple_with_column_parallel() -> DaftResult<()> {
        let resource_request = create_resource_request();
        let scan_op = dummy_scan_operator(vec![Field::new("a", daft_core::DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stateful_project_expr = create_stateful_udf(vec![col("a")]);

        // Add a Projection with StatefulUDF and resource request
        let project_plan = scan_plan
            .with_columns(
                vec![
                    stateful_project_expr.clone().alias("b"),
                    stateful_project_expr.clone().alias("c"),
                ],
                resource_request.clone(),
            )?
            .build();

        // Project([col("a")])
        //   --> ActorPoolProject([col("a"), foo(col("a")).alias("SOME_FACTORED_NAME")])
        //   --> Project([col("a"), col("SOME_FACTORED_NAME").alias("b"), foo(col("SOME_FACTORED_NAME")).alias("c")])
        let factored_column_name = "Function_Python(Stateful(StatefulPythonUDF { name: \"foo\", num_expressions: 1, return_dtype: Binary }))(a)";
        let expected = scan_plan.select(vec![col("a").alias("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                stateful_project_expr.clone().alias(factored_column_name),
            ],
            // Actor pool project has the specified resource request, but the normal Project has the default resource request
            resource_request.clone(),
            NUM_ACTORS,
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                col("a"),
                col(factored_column_name).alias("b"),
                col(factored_column_name).alias("c"),
            ],
            resource_request.clone(),
        )?)
        .arced();

        assert_optimized_plan_eq(project_plan, expected)?;

        Ok(())
    }
}
