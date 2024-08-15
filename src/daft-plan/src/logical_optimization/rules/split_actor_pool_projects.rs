use std::{collections::HashSet, iter, sync::Arc};

use common_error::DaftResult;
use common_treenode::{TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use daft_dsl::{
    functions::{
        python::{PythonUDF, StatefulPythonUDF},
        FunctionExpr,
    },
    optimization::{get_required_columns, requires_computation},
    Expr, ExprRef,
};
use itertools::Itertools;

use crate::{
    logical_ops::{ActorPoolProject, Project},
    LogicalPlan,
};

use super::{ApplyOrder, OptimizerRule, Transformed};

#[derive(Default, Debug)]
pub struct SplitActorPoolProjects {}

impl SplitActorPoolProjects {
    pub fn new() -> Self {
        Self {}
    }
}

/// Implement SplitActorPoolProjects as an OptimizerRule which will:
///
/// 1. Go top-down from the root of the LogicalPlan
/// 2. Whenever it sees a Project with StatefulUDF(s), it will split it like so:
///      Project_recursive (optional) -> Project_stateless -> ActorPoolProject(s)... -> Project_final
/// 3. Then it recurses on `Project_recursive` until there is no more `Project_recursive` to split anymore
///
/// Invariants:
/// * `Project_recursive` definitely contains at least 1 stateful UDF, and hence need to be recursively split. If it is not constructed, then this is the base case.
/// * `Project_stateless` contains: [...stateless_projections, ...passthrough_columns_as_colexprs]
/// * Subsequent `ActorPoolProject(s)` contain: [Single StatefulUDF, ...passthrough_columns_as_colexprs]
/// * `Project_final` contains only Expr::Columns, and has the same column names (and column ordering) as the original Projection
/// * At the end of splitting, all Project nodes will never contain a StatefulUDF, and all ActorPoolProject nodes will contain one-and-only-one StatefulUDF
///
/// How splitting is performed on a given Project:
/// 1. For every expression in the Project, "skim off the top"
///     * If the root expression is a StatefulUDF, truncate all of its children, alias them, and then add them to `Project_recursive`
///     * If the root expression is not a StatefulUDF, truncate any StatefulUDF children, alias them, and add them to `Project_recursive`
/// 2. Recursively perform splitting on `Project_recursive`
/// 3. Now for the current truncated expressions, split them into stateless vs stateful expressions:
///     * All stateless expressions go into a single `Project_stateless` node
///     * For each stateful expression, they go into their own dedicated `ActorPoolProject` node
///     * The final `Project_final` node is naively constructed using the names of the original Project
impl OptimizerRule for SplitActorPoolProjects {
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::TopDown
    }

    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Project(projection) => try_optimize_project(projection, plan.clone(), 0),
            // TODO: Figure out how to split other nodes as well such as Filter, Agg etc
            _ => Ok(Transformed::No(plan)),
        }
    }
}

// TreeNodeRewriter that assumes the Expression tree is rooted at a StatefulUDF (or alias of a StatefulUDF)
// and its children need to be truncated + replaced with Expr::Columns
struct TruncateRootStatefulUDF {
    pub(crate) new_children: Vec<ExprRef>,
    stage_idx: usize,
    expr_idx: usize,
}

impl TruncateRootStatefulUDF {
    fn new(stage_idx: usize, expr_idx: usize) -> Self {
        Self {
            new_children: Vec::new(),
            stage_idx,
            expr_idx,
        }
    }
}

// TreeNodeRewriter that assumes the Expression tree has some children which are StatefulUDFs
// which needs to be truncated and replaced with Expr::Columns
struct TruncateAnyStatefulUDFChildren {
    pub(crate) new_children: Vec<ExprRef>,
    stage_idx: usize,
    expr_idx: usize,
}

impl TruncateAnyStatefulUDFChildren {
    fn new(stage_idx: usize, expr_idx: usize) -> Self {
        Self {
            new_children: Vec::new(),
            stage_idx,
            expr_idx,
        }
    }
}

impl TreeNodeRewriter for TruncateRootStatefulUDF {
    type Node = ExprRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            // If we encounter a ColumnExpr, we add it to new_children only if it hasn't already been accounted for
            Expr::Column(name) => {
                if !self
                    .new_children
                    .iter()
                    .map(|e| e.name())
                    .contains(&name.as_ref())
                {
                    self.new_children.push(node.clone());
                }
                Ok(common_treenode::Transformed::no(node))
            }
            // Encountered stateful UDF: chop off all children and add to self.next_children
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF { .. })),
                inputs,
            } => {
                let mut monotonically_increasing_expr_identifier = 0;
                let new_inputs = inputs.iter().map(|e| {
                    if requires_computation(e.as_ref()) {
                        // Give the new child a deterministic name
                        let intermediate_expr_name = format!(
                            "__TruncateRootStatefulUDF_{}-{}-{}__",
                            self.stage_idx, self.expr_idx, monotonically_increasing_expr_identifier
                        );
                        monotonically_increasing_expr_identifier += 1;

                        self.new_children
                            .push(e.clone().alias(intermediate_expr_name.as_str()));
                        Expr::Column(intermediate_expr_name.as_str().into()).arced()
                    } else {
                        e.clone()
                    }
                });
                let new_truncated_node = node.with_new_children(new_inputs.collect()).arced();
                Ok(common_treenode::Transformed::yes(new_truncated_node))
            }
            _ => Ok(common_treenode::Transformed::no(node)),
        }
    }
}

impl TreeNodeRewriter for TruncateAnyStatefulUDFChildren {
    type Node = ExprRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            // This rewriter should never encounter a StatefulUDF expression (they should always be truncated and replaced)
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF { .. })),
                ..
            } => {
                unreachable!(
                    "TruncateAnyStatefulUDFChildren should never run on a StatefulUDF expression"
                );
            }
            // If we encounter a ColumnExpr, we add it to new_children only if it hasn't already been accounted for
            Expr::Column(name) => {
                if !self
                    .new_children
                    .iter()
                    .map(|e| e.name())
                    .contains(&name.as_ref())
                {
                    self.new_children.push(node.clone());
                }
                Ok(common_treenode::Transformed::no(node))
            }
            // Attempt to truncate any children that are StatefulUDFs, replacing them with a Expr::Column
            expr => {
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

                let mut monotonically_increasing_expr_identifier = 0;
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
                        let intermediate_expr_name = format!(
                            "__TruncateAnyStatefulUDFChildren_{}-{}-{}__",
                            self.stage_idx, self.expr_idx, monotonically_increasing_expr_identifier
                        );
                        monotonically_increasing_expr_identifier += 1;

                        self.new_children
                            .push(e.clone().alias(intermediate_expr_name.as_str()));
                        Expr::Column(intermediate_expr_name.as_str().into()).arced()
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

/// Splits a projection down into two sets of new projections: (truncated_exprs, new_children)
///
/// `truncated_exprs` are the newly truncated expressions from `projection`. This has the same
/// length as `projection`, and also the same names as each Expr in `projection`. However, their
/// children are (potentially) truncated and replaced with Expr::Column nodes, which refer to
/// Exprs in `new_children`.
///
/// `new_children` are the new children of `truncated_exprs`. Every Expr::Column leaf node in
/// `truncated_exprs` should have a corresponding expression in `new_children` with the appropriate
/// name.
fn split_projection(
    projection: &[ExprRef],
    stage_idx: usize,
) -> DaftResult<(Vec<ExprRef>, Vec<ExprRef>)> {
    let mut truncated_exprs = Vec::new();
    let (mut new_children_seen, mut new_children): (HashSet<String>, Vec<ExprRef>) =
        (HashSet::new(), Vec::new());

    fn _is_stateful_udf_and_should_truncate_children(expr: &ExprRef) -> bool {
        let mut is_stateful_udf = true;
        expr.apply(|e| match e.as_ref() {
            Expr::Alias(..) => Ok(TreeNodeRecursion::Continue),
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF { .. })),
                ..
            } => Ok(TreeNodeRecursion::Stop),
            _ => {
                is_stateful_udf = false;
                Ok(TreeNodeRecursion::Stop)
            }
        })
        .unwrap();
        is_stateful_udf
    }

    for (expr_idx, expr) in projection.iter().enumerate() {
        // Run the TruncateRootStatefulUDF TreeNodeRewriter
        if _is_stateful_udf_and_should_truncate_children(expr) {
            let mut rewriter = TruncateRootStatefulUDF::new(stage_idx, expr_idx);
            let rewritten_root = expr.clone().rewrite(&mut rewriter)?.data;
            truncated_exprs.push(rewritten_root);
            for new_child in rewriter.new_children {
                if !new_children_seen.contains(new_child.name()) {
                    new_children_seen.insert(new_child.name().to_string());
                    new_children.push(new_child.clone());
                }
            }

        // Run the TruncateAnyStatefulUDFChildren TreeNodeRewriter
        } else if has_stateful_udf(expr) {
            let mut rewriter = TruncateAnyStatefulUDFChildren::new(stage_idx, expr_idx);
            let rewritten_root = expr.clone().rewrite(&mut rewriter)?.data;
            truncated_exprs.push(rewritten_root);
            for new_child in rewriter.new_children {
                if !new_children_seen.contains(new_child.name()) {
                    new_children_seen.insert(new_child.name().to_string());
                    new_children.push(new_child.clone());
                }
            }

        // No need to rewrite the tree
        } else {
            truncated_exprs.push(expr.clone());
            for required_col_name in get_required_columns(expr) {
                if !new_children_seen.contains(&required_col_name) {
                    let colexpr = Expr::Column(required_col_name.as_str().into()).arced();
                    new_children_seen.insert(required_col_name);
                    new_children.push(colexpr);
                }
            }
        }
    }

    Ok((truncated_exprs, new_children))
}

fn try_optimize_project(
    projection: &Project,
    plan: Arc<LogicalPlan>,
    recursive_count: usize,
) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
    // Base case: no stateful UDFs at all
    let has_stateful_udfs = projection.projection.iter().any(has_stateful_udf);
    if !has_stateful_udfs {
        return Ok(Transformed::No(plan));
    }

    log::debug!(
        "Optimizing: {}",
        projection
            .projection
            .iter()
            .map(|e| e.as_ref().to_string())
            .join(", ")
    );

    // Split the Projection into:
    // * remaining: remaining parts of the Project to recurse on
    // * truncated_exprs: current parts of the Project to split into (Project -> ActorPoolProjects -> Project)
    let (truncated_exprs, remaining): (Vec<ExprRef>, Vec<ExprRef>) =
        split_projection(projection.projection.as_slice(), recursive_count)?;

    log::debug!(
        "Truncated Exprs: {}",
        truncated_exprs
            .iter()
            .map(|e| e.as_ref().to_string())
            .join(", ")
    );
    log::debug!(
        "Remaining: {}",
        remaining.iter().map(|e| e.as_ref().to_string()).join(", ")
    );

    // Recurse if necessary (if there are any non-noop expressions left to run in `remaining`)
    let new_plan_child = if remaining
        .iter()
        .all(|e| matches!(e.as_ref(), Expr::Column(_)))
    {
        // Nothing remaining, we're done splitting and should wire the new node up with the child of the Project
        plan.children()[0].clone()
    } else {
        // Recursively run the rule on the new child Project
        let new_project = Project::try_new(plan.children()[0].clone(), remaining)?;
        let new_child_project = LogicalPlan::Project(new_project.clone()).arced();
        let optimized_child_plan =
            try_optimize_project(&new_project, new_child_project.clone(), recursive_count + 1)?;
        optimized_child_plan.unwrap().clone()
    };

    // Start building a chain of `child -> Project -> ActorPoolProject -> ActorPoolProject -> ... -> Project`
    let (stateful_stages, stateless_stages): (Vec<_>, Vec<_>) =
        truncated_exprs.into_iter().partition(has_stateful_udf);

    // Build the new stateless Project: [...all columns that came before it, ...stateless_projections]
    let passthrough_columns = {
        let stateless_stages_names: HashSet<String> = stateless_stages
            .iter()
            .map(|e| e.name().to_string())
            .collect();
        new_plan_child
            .schema()
            .names()
            .into_iter()
            .filter_map(|name| {
                if stateless_stages_names.contains(name.as_str()) {
                    None
                } else {
                    Some(Expr::Column(name.as_str().into()).arced())
                }
            })
            .collect_vec()
    };
    let stateless_projection = passthrough_columns
        .into_iter()
        .chain(stateless_stages)
        .collect();
    let new_plan = LogicalPlan::Project(Project::try_new(
        new_plan_child.clone(),
        stateless_projection,
    )?)
    .arced();

    // Iteratively build ActorPoolProject nodes: [...all columns that came before it, StatefulUDF]
    let new_plan = {
        let mut child = new_plan;

        for stateful_expr in stateful_stages {
            let stateful_expr_name = stateful_expr.name().to_string();
            let stateful_projection = child
                .schema()
                .fields
                .iter()
                .filter_map(|(name, _)| {
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
            )?)
            .arced();
        }
        child
    };

    // One final project to select just the columns we need
    // This will help us do the necessary column pruning via projection pushdowns
    let final_selection_project = LogicalPlan::Project(Project::try_new(
        new_plan,
        projection
            .projection
            .iter()
            .map(|e| Expr::Column(e.name().into()).arced())
            .collect(),
    )?)
    .arced();

    Ok(Transformed::Yes(final_selection_project))
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
    use test_log::test;

    use common_error::DaftResult;
    use common_resource_request::ResourceRequest;
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
        logical_optimization::{
            rules::PushDownProjection, test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
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

    /// Helper that creates an optimizer with the SplitExprByStatefulUDF rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq_with_projection_pushdown(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![
                Box::new(SplitActorPoolProjects {}),
                Box::new(PushDownProjection::new()),
            ],
        )
    }

    fn create_stateful_udf(inputs: Vec<ExprRef>) -> ExprRef {
        Expr::Function {
            func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                name: Arc::new("foo".to_string()),
                num_expressions: inputs.len(),
                return_dtype: daft_core::DataType::Int64,
                resource_request: Some(create_resource_request()),
                batch_size: None,
                concurrency: Some(8),
            })),
            inputs,
        }
        .arced()
    }

    fn create_resource_request() -> ResourceRequest {
        ResourceRequest {
            num_cpus: Some(8.),
            num_gpus: Some(1.),
            memory_bytes: None,
        }
    }

    #[test]
    fn test_with_column_stateful_udf_happypath() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", daft_core::DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stateful_project_expr = create_stateful_udf(vec![col("a")]);

        // Add a Projection with StatefulUDF and resource request
        let project_plan = scan_plan
            .with_columns(vec![stateful_project_expr.clone().alias("b")])?
            .build();

        // Project([col("a")]) --> ActorPoolProject([col("a"), foo(col("a")).alias("b")]) --> Project([col("a"), col("b")])
        let expected = scan_plan.select(vec![col("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![col("a"), stateful_project_expr.clone().alias("b")],
        )?)
        .arced();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![col("a"), col("b")])?).arced();

        assert_optimized_plan_eq(project_plan, expected)?;

        Ok(())
    }

    #[test]
    fn test_multiple_with_column_parallel() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", daft_core::DataType::Utf8),
            Field::new("b", daft_core::DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node(scan_op);
        let project_plan = scan_plan
            .with_columns(vec![
                create_stateful_udf(vec![create_stateful_udf(vec![col("a")])]).alias("a_prime"),
                create_stateful_udf(vec![create_stateful_udf(vec![col("b")])]).alias("b_prime"),
            ])?
            .build();

        let intermediate_column_name_0 = "__TruncateRootStatefulUDF_0-2-0__";
        let intermediate_column_name_1 = "__TruncateRootStatefulUDF_0-3-0__";
        let expected = scan_plan.select(vec![col("a"), col("b")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                col("b"),
                create_stateful_udf(vec![col("a")]).alias(intermediate_column_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                col("b"),
                col(intermediate_column_name_0),
                create_stateful_udf(vec![col("b")]).alias(intermediate_column_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                col("a"),
                col("b"),
                col(intermediate_column_name_0),
                col(intermediate_column_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                col(intermediate_column_name_0),
                col(intermediate_column_name_1),
                col("a"),
                col("b"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col(intermediate_column_name_0),
                col(intermediate_column_name_1),
                col("a"),
                col("b"),
                create_stateful_udf(vec![col(intermediate_column_name_0)]).alias("a_prime"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col(intermediate_column_name_0),
                col(intermediate_column_name_1),
                col("a"),
                col("b"),
                col("a_prime"),
                create_stateful_udf(vec![col(intermediate_column_name_1)]).alias("b_prime"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col("a"), col("b"), col("a_prime"), col("b_prime")],
        )?)
        .arced();
        assert_optimized_plan_eq(project_plan, expected)?;
        Ok(())
    }

    #[test]
    fn test_multiple_with_column_parallel_common_subtree_eliminated() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", daft_core::DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stateful_project_expr = create_stateful_udf(vec![col("a")]);

        // Add a Projection with StatefulUDF and resource request
        // NOTE: Our common-subtree elimination will build this as 2 project nodes:
        // Project([col("a").alias("a"), foo(col("a")).alias(factored_column_name)])
        //   --> Project([col("a"), col(factored_column_name).alias("b"), col(factored_column_name).alias("c")])
        let factored_column_name = "Function_Python(Stateful(StatefulPythonUDF { name: \"foo\", num_expressions: 1, return_dtype: Int64, resource_request: Some(ResourceRequest { num_cpus: Some(8.0), num_gpus: Some(1.0), memory_bytes: None }), batch_size: None, concurrency: Some(8) }))(a)";
        let project_plan = scan_plan
            .with_columns(vec![
                stateful_project_expr.clone().alias("b"),
                stateful_project_expr.clone().alias("c"),
            ])?
            .build();

        let expected = scan_plan.select(vec![col("a").alias("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                stateful_project_expr.clone().alias(factored_column_name),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col("a"), col(factored_column_name)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                col("a"),
                col(factored_column_name).alias("b"),
                col(factored_column_name).alias("c"),
            ],
        )?)
        .arced();
        assert_optimized_plan_eq(project_plan.clone(), expected.clone())?;

        // With Projection Pushdown, elide intermediate Projects and also perform column pushdown
        let expected = scan_plan.select(vec![col("a").alias("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                stateful_project_expr.clone().alias(factored_column_name),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                col("a"),
                col(factored_column_name).alias("b"),
                col(factored_column_name).alias("c"),
            ],
        )?)
        .arced();
        assert_optimized_plan_eq_with_projection_pushdown(project_plan, expected)?;

        Ok(())
    }

    #[test]
    fn test_multiple_with_column_serial() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", daft_core::DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_stateful_project_expr =
            create_stateful_udf(vec![create_stateful_udf(vec![col("a")])]);

        // Add a Projection with StatefulUDF and resource request
        // Project([col("a"), foo(foo(col("a"))).alias("b")])
        let project_plan = scan_plan
            .with_columns(vec![stacked_stateful_project_expr.clone().alias("b")])?
            .build();

        let intermediate_name = "__TruncateRootStatefulUDF_0-1-0__";
        let expected = scan_plan.select(vec![col("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                create_stateful_udf(vec![col("a")])
                    .clone()
                    .alias(intermediate_name),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col("a"), col(intermediate_name)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col(intermediate_name), col("a")],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col(intermediate_name),
                col("a"),
                create_stateful_udf(vec![col(intermediate_name)])
                    .clone()
                    .alias("b"),
            ],
        )?)
        .arced();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![col("a"), col("b")])?).arced();
        assert_optimized_plan_eq(project_plan.clone(), expected.clone())?;

        // With Projection Pushdown, elide intermediate Projects and also perform column pushdown
        let expected = scan_plan.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                create_stateful_udf(vec![col("a")])
                    .clone()
                    .alias(intermediate_name),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col(intermediate_name), col("a")],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                create_stateful_udf(vec![col(intermediate_name)])
                    .clone()
                    .alias("b"),
            ],
        )?)
        .arced();
        assert_optimized_plan_eq_with_projection_pushdown(project_plan, expected)?;
        Ok(())
    }

    #[test]
    fn test_multiple_with_column_serial_multiarg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", daft_core::DataType::Utf8),
            Field::new("b", daft_core::DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_stateful_project_expr = create_stateful_udf(vec![
            create_stateful_udf(vec![col("a")]),
            create_stateful_udf(vec![col("b")]),
        ]);

        // Add a Projection with StatefulUDF and resource request
        // Project([foo(foo(col("a")), foo(col("b"))).alias("c")])
        let project_plan = scan_plan
            .select(vec![stacked_stateful_project_expr.clone().alias("c")])?
            .build();

        let intermediate_name_0 = "__TruncateRootStatefulUDF_0-0-0__";
        let intermediate_name_1 = "__TruncateRootStatefulUDF_0-0-1__";
        let expected = scan_plan.select(vec![col("a"), col("b")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                col("b"),
                create_stateful_udf(vec![col("a")])
                    .clone()
                    .alias(intermediate_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                col("b"),
                col(intermediate_name_0),
                create_stateful_udf(vec![col("b")])
                    .clone()
                    .alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col(intermediate_name_0), col(intermediate_name_1)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col(intermediate_name_0), col(intermediate_name_1)],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col(intermediate_name_0),
                col(intermediate_name_1),
                create_stateful_udf(vec![col(intermediate_name_0), col(intermediate_name_1)])
                    .clone()
                    .alias("c"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(expected, vec![col("c")])?).arced();
        assert_optimized_plan_eq(project_plan.clone(), expected.clone())?;

        // With Projection Pushdown, elide intermediate Projects and also perform column pushdown
        let expected = scan_plan.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"), // TODO: This should be able to be pruned as well, but it seems Projection Pushdown isn't working as intended
                col("b"),
                create_stateful_udf(vec![col("a")])
                    .clone()
                    .alias(intermediate_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col(intermediate_name_0),
                create_stateful_udf(vec![col("b")])
                    .clone()
                    .alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                create_stateful_udf(vec![col(intermediate_name_0), col(intermediate_name_1)])
                    .clone()
                    .alias("c"),
            ],
        )?)
        .arced();
        assert_optimized_plan_eq_with_projection_pushdown(project_plan.clone(), expected.clone())?;
        Ok(())
    }

    #[test]
    fn test_multiple_with_column_serial_multiarg_with_intermediate_stateless() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", daft_core::DataType::Int64),
            Field::new("b", daft_core::DataType::Int64),
        ]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_stateful_project_expr = create_stateful_udf(vec![create_stateful_udf(vec![
            col("a"),
        ])
        .add(create_stateful_udf(vec![col("b")]))]);

        // Add a Projection with StatefulUDF and resource request
        // Project([foo(foo(col("a")) + foo(col("b"))).alias("c")])
        let project_plan = scan_plan
            .select(vec![stacked_stateful_project_expr.clone().alias("c")])?
            .build();

        let intermediate_name_0 = "__TruncateAnyStatefulUDFChildren_1-0-0__";
        let intermediate_name_1 = "__TruncateAnyStatefulUDFChildren_1-0-1__";
        let intermediate_name_2 = "__TruncateRootStatefulUDF_0-0-0__";
        let expected = scan_plan.select(vec![col("a"), col("b")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                col("b"),
                create_stateful_udf(vec![col("a")])
                    .clone()
                    .alias(intermediate_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                col("b"),
                col(intermediate_name_0),
                create_stateful_udf(vec![col("b")])
                    .clone()
                    .alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col(intermediate_name_0), col(intermediate_name_1)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                col(intermediate_name_0),
                col(intermediate_name_1),
                col(intermediate_name_0)
                    .add(col(intermediate_name_1))
                    .alias(intermediate_name_2),
            ],
        )?)
        .arced();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![col(intermediate_name_2)])?)
                .arced();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![col(intermediate_name_2)])?)
                .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col(intermediate_name_2),
                create_stateful_udf(vec![col(intermediate_name_2)])
                    .clone()
                    .alias("c"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(expected, vec![col("c")])?).arced();
        assert_optimized_plan_eq(project_plan.clone(), expected.clone())?;

        // With Projection Pushdown, elide intermediate Projects and also perform column pushdown
        let expected = scan_plan.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"), // TODO: This should be pruned by Projection Pushdown, but isn't for some reason
                col("b"),
                create_stateful_udf(vec![col("a")])
                    .clone()
                    .alias(intermediate_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col(intermediate_name_0),
                create_stateful_udf(vec![col("b")])
                    .clone()
                    .alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col(intermediate_name_0)
                .add(col(intermediate_name_1))
                .alias(intermediate_name_2)],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![create_stateful_udf(vec![col(intermediate_name_2)])
                .clone()
                .alias("c")],
        )?)
        .arced();
        assert_optimized_plan_eq_with_projection_pushdown(project_plan.clone(), expected.clone())?;
        Ok(())
    }

    #[test]
    fn test_nested_with_column_same_names() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", daft_core::DataType::Int64)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_stateful_project_expr =
            create_stateful_udf(vec![col("a").add(create_stateful_udf(vec![col("a")]))]);

        // Add a Projection with StatefulUDF and resource request
        // Project([foo(col("a") + foo(col("a"))).alias("c")])
        let project_plan = scan_plan
            .select(vec![
                col("a"),
                stacked_stateful_project_expr.clone().alias("c"),
            ])?
            .build();

        let intermediate_name_0 = "__TruncateAnyStatefulUDFChildren_1-1-0__";
        let intermediate_name_1 = "__TruncateRootStatefulUDF_0-1-0__";
        let expected = scan_plan.build();
        let expected = LogicalPlan::Project(Project::try_new(expected, vec![col("a")])?).arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                create_stateful_udf(vec![col("a")]).alias(intermediate_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col("a"), col(intermediate_name_0)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                col(intermediate_name_0),
                col("a"),
                col("a")
                    .add(col(intermediate_name_0))
                    .alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col("a"), col(intermediate_name_1)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col(intermediate_name_1), col("a")],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col(intermediate_name_1),
                col("a"),
                create_stateful_udf(vec![col(intermediate_name_1)]).alias("c"),
            ],
        )?)
        .arced();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![col("a"), col("c")])?).arced();

        assert_optimized_plan_eq(project_plan.clone(), expected.clone())?;

        Ok(())
    }
}
