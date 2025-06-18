use std::{collections::HashSet, iter, sync::Arc};

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use daft_dsl::{
    is_actor_pool_udf,
    optimization::{get_required_columns, requires_computation},
    resolved_col, Column, Expr, ExprRef, ResolvedColumn,
};
use itertools::Itertools;

use super::OptimizerRule;
use crate::{
    ops::{ActorPoolProject, Project},
    LogicalPlan,
};

#[derive(Default, Debug)]
pub struct SplitActorPoolProjects {}

impl SplitActorPoolProjects {
    pub fn new() -> Self {
        Self {}
    }
}

/// Implement SplitActorPoolProjects as an OptimizerRule
/// * Splits PROJECT nodes into chains of (PROJECT -> ...ACTOR_POOL_PROJECTS -> PROJECT) ...
/// * Resultant PROJECT nodes will never contain any actor pool UDF expressions
/// * Each ACTOR_POOL_PROJECT node only contains a single actor pool UDF expression
///
/// Given a projection with 3 expressions that look like the following:
///
/// ┌─────────────────────────────────────────────── PROJECTION ────┐
/// │                                                               │
/// │        ┌─────┐              ┌─────┐              ┌─────┐      │
/// │        │ E1  │              │ E2  │              │ E3  │      │
/// │        │     │              │     │              │     │      │
/// │       UDF    │           Stateless│           Stateless│      │
/// │        └──┬──┘              └─┬┬──┘              └──┬──┘      │
/// │           │                ┌──┘└──┐                 │         │
/// │        ┌──▼──┐         ┌───▼─┐  ┌─▼───┐          ┌──▼────┐    │
/// │        │ E1a │         │ E2a │  │ E2b │          │col(E3)│    │
/// │        │     │         │     │  │     │          └───────┘    │
/// │       Any    │        UDF    │  │ Stateless                   │
/// │        └─────┘         └─────┘  └─────┘                       │
/// │                                                               │
/// └───────────────────────────────────────────────────────────────┘
///
/// We will attempt to split this recursively into "stages". We split a given projection by truncating each expression as follows:
///
/// 1. (See E1 -> E1') Expressions with (aliased) actor pool UDFs as root nodes have all their children truncated
/// 2. (See E2 -> E2') Expressions with children actor pool UDFs have each child actor pool UDF truncated
/// 3. (See E3) Expressions without any actor pool UDFs at all are not modified
///
/// The truncated children as well as any required `col` references are collected into a new set of [`remaining`]
/// expressions. The new [`truncated_exprs`] make up current stage, and the [`remaining`] exprs represent the projections
/// from prior stages that will need to be recursively split into more stages.
///
/// ┌───────────────────────────────────────────────────────────SPLIT: split_projection()
/// │                                                                                 │
/// │   TruncateRootActorPoolUDF     TruncateAnyActorPoolUDFChildren        No-Op       │
/// │   =======================     ==============================        =====       │
/// │           ┌─────┐                       ┌─────┐                    ┌─────┐      │
/// │           │ E1' │                       │ E2' │                    │ E3  │      │
/// │          UDF    │                    Stateless│                 Stateless│      │
/// │           └───┬─┘                       └─┬┬──┘                    └──┬──┘      │
/// │               │                       ┌───┘└───┐                      │         │
/// │          *--- ▼---*              *--- ▼---* ┌──▼──┐                ┌──▼────┐    │
/// │         / col(x) /              / col(y) /  │ E2b │                │col(E3)│    │
/// │        *--------*              *--------*   │ Stateless            └───────┘    │
/// │                                             └─────┘                             │
/// │                                                                                 │
/// │ [`truncated_exprs`]                                                             │
/// ├─- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -─┤
/// │ [`remaining`]                                                                   │
/// │                                                                                 │
/// │          *----------*         *----------*  ┌────────┐             ┌───────┐    │
/// │         / alias(x) /         / alias(y) /   │col(E2b)│             │col(E3)│    │
/// │        *-----│----*         *-------│--*    └────────┘             └───────┘    │
/// │           ┌──▼──┐               ┌───▼─┐                                         │
/// │           │ E1a │               │ E2a │                                         │
/// │           │     │               │     │                                         │
/// │          Any    │              UDF    │                                         │
/// │           └─────┘               └─────┘                                         │
/// └─────────────────────────────────────────────────────────────────────────────────┘
///
/// We then perform recursion on [`remaining`] until [`remaining`] becomes only `col` references,
/// as this would indicate that no further work needs to be performed by the projection.
///
/// ┌───────────────────────────── Recursively split [`remaining`] ─┐
/// │                                                               │
/// │     *----------*     *----------*   ┌────────┐   ┌───────┐    │
/// │    / alias(x) /     / alias(y) /    │col(E2b)│   │col(E3)│    │
/// │   *-----│----*     *-------│--*     └────────┘   └───────┘    │
/// │      ┌──▼──┐           ┌───▼─┐                                │
/// │      │ E1a │           │ E2a │                                │
/// │      │     │           │     │                                │
/// │     Any    │          UDF    │                                │
/// │      └─────┘           └─────┘                                │
/// │                                                               │
/// └─┬─────────────────────────────────────────────────────────────┘
///   |
///   │    Then, we link this up with our current stage, which will be resolved into a chain of logical nodes:
///   |    * The first PROJECT contains all the stateless expressions (E2' and E3) and passes through all required columns.
///   |    * Subsequent ACTOR_POOL_PROJECT nodes each contain only one actor pool UDF, and passes through all required columns.
///   |    * The last PROJECT contains only `col` references, and correctly orders/prunes columns according to the original projection.
///   |
///   │
///   │    [`truncated_exprs`] resolved as a chain of logical nodes:
///   │    ┌─────────────────┐  ┌────────────────────┐                 ┌───────────┐
///   │    │ PROJECT         │  │ ACTOR_POOL_PROJECT │                 │ PROJECT   │
///   │    │ -------         │  │ ------------------ │  ...ACTOR_PPs,  │ ----------│
///   └───►│ E2', E3, col(*) ├─►│ E1', col(*)        ├─ 1 per each   ─►│ col("e1") │
///        │                 │  │                    │  actor pool UDF │ col("e2") │
///        │                 │  │                    │                 │ col("e3") │
///        │                 │  │                    │                 │           │
///        └─────────────────┘  └────────────────────┘                 └───────────┘
impl OptimizerRule for SplitActorPoolProjects {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| match node.as_ref() {
            LogicalPlan::Project(projection) => try_optimize_project(projection, node.clone()),
            _ => Ok(Transformed::no(node)),
        })
    }
}

// TreeNodeRewriter that assumes the Expression tree is rooted at a actor pool UDF (or alias of a actor pool UDF)
// and its children need to be truncated + replaced with Expr::Columns
struct TruncateRootActorPoolUDF {
    pub(crate) new_children: Vec<ExprRef>,
    stage_idx: usize,
    expr_idx: usize,
}

impl TruncateRootActorPoolUDF {
    fn new(stage_idx: usize, expr_idx: usize) -> Self {
        Self {
            new_children: Vec::new(),
            stage_idx,
            expr_idx,
        }
    }
}

// TreeNodeRewriter that assumes the Expression tree has some children which are actor pool UDFs
// which needs to be truncated and replaced with Expr::Columns
struct TruncateAnyActorPoolUDFChildren {
    pub(crate) new_children: Vec<ExprRef>,
    stage_idx: usize,
    expr_idx: usize,
}

impl TruncateAnyActorPoolUDFChildren {
    fn new(stage_idx: usize, expr_idx: usize) -> Self {
        Self {
            new_children: Vec::new(),
            stage_idx,
            expr_idx,
        }
    }
}

/// Performs truncation of Expressions which are assumed to be rooted at a actor pool UDF expression
///
/// This TreeNodeRewriter will truncate all children of the actor pool UDF expression like so:
///
/// 1. Add an `alias(...)` to the child and push it onto `self.new_children`
/// 2. Replace the child with a `col("...")`
/// 3. Add any `col("...")` leaf nodes to `self.new_children` (only once per unique column name)
impl TreeNodeRewriter for TruncateRootActorPoolUDF {
    type Node = ExprRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            // If we encounter a ColumnExpr, we add it to new_children only if it hasn't already been accounted for
            Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => {
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
            // Encountered actor pool UDF: chop off all children and add to self.next_children
            _ if is_actor_pool_udf(&node) => {
                let mut monotonically_increasing_expr_identifier = 0;
                let inputs = node.children();
                let new_inputs = inputs.iter().map(|e| {
                    if requires_computation(e.as_ref()) {
                        // Give the new child a deterministic name
                        let intermediate_expr_name = format!(
                            "__TruncateRootActorPoolUDF_{}-{}-{}__",
                            self.stage_idx, self.expr_idx, monotonically_increasing_expr_identifier
                        );
                        monotonically_increasing_expr_identifier += 1;

                        self.new_children
                            .push(e.clone().alias(intermediate_expr_name.as_str()));

                        resolved_col(intermediate_expr_name)
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

/// Performs truncation of Expressions which are assumed to have some subtrees which contain actor pool UDF expressions
///
/// This TreeNodeRewriter will truncate actor pool UDF expressions from the tree like so:
///
/// 1. Add an `alias(...)` to any actor pool UDF child and push it onto `self.new_children`
/// 2. Replace the child with a `col("...")`
/// 3. Add any `col("...")` leaf nodes to `self.new_children` (only once per unique column name)
impl TreeNodeRewriter for TruncateAnyActorPoolUDFChildren {
    type Node = ExprRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            // This rewriter should never encounter a actor pool UDF expression (they should always be truncated and replaced)
            _ if is_actor_pool_udf(&node) => {
                unreachable!(
                    "TruncateAnyActorPoolUDFChildren should never run on a actor pool UDF expression"
                );
            }
            // If we encounter a ColumnExpr, we add it to new_children only if it hasn't already been accounted for
            Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => {
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
            // Attempt to truncate any children that are actor pool UDFs, replacing them with a Expr::Column
            expr => {
                // None of the direct children are actor pool UDFs, so we keep going
                if !node.children().iter().any(is_actor_pool_udf) {
                    return Ok(common_treenode::Transformed::no(node));
                }

                let mut monotonically_increasing_expr_identifier = 0;
                let inputs = expr.children();
                let new_inputs = inputs.iter().map(|e| {
                    if is_actor_pool_udf(e) {
                        let intermediate_expr_name = format!(
                            "__TruncateAnyActorPoolUDFChildren_{}-{}-{}__",
                            self.stage_idx, self.expr_idx, monotonically_increasing_expr_identifier
                        );
                        monotonically_increasing_expr_identifier += 1;

                        self.new_children
                            .push(e.clone().alias(intermediate_expr_name.as_str()));

                        resolved_col(intermediate_expr_name)
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
fn split_projection(
    projection: &[ExprRef],
    stage_idx: usize,
) -> DaftResult<(Vec<ExprRef>, Vec<ExprRef>)> {
    let mut truncated_exprs = Vec::new();
    let (mut new_children_seen, mut new_children): (HashSet<String>, Vec<ExprRef>) =
        (HashSet::new(), Vec::new());

    fn is_actor_pool_udf_and_should_truncate_children(expr: &ExprRef) -> bool {
        let mut cond = true;
        expr.apply(|e| match e.as_ref() {
            Expr::Alias(..) => Ok(TreeNodeRecursion::Continue),
            _ if is_actor_pool_udf(e) => Ok(TreeNodeRecursion::Stop),
            _ => {
                cond = false;
                Ok(TreeNodeRecursion::Stop)
            }
        })
        .unwrap();
        cond
    }

    for (expr_idx, expr) in projection.iter().enumerate() {
        // Run the TruncateRootActorPoolUDF TreeNodeRewriter
        if is_actor_pool_udf_and_should_truncate_children(expr) {
            let mut rewriter = TruncateRootActorPoolUDF::new(stage_idx, expr_idx);
            let rewritten_root = expr.clone().rewrite(&mut rewriter)?.data;
            truncated_exprs.push(rewritten_root);
            for new_child in rewriter.new_children {
                if !new_children_seen.contains(new_child.name()) {
                    new_children_seen.insert(new_child.name().to_string());
                    new_children.push(new_child.clone());
                }
            }

        // Run the TruncateAnyActorPoolUDFChildren TreeNodeRewriter
        } else if expr.exists(is_actor_pool_udf) {
            let mut rewriter = TruncateAnyActorPoolUDFChildren::new(stage_idx, expr_idx);
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
                #[expect(
                    clippy::set_contains_or_insert,
                    reason = "we are arcing it later; we might want to use contains separately unless there is a better way"
                )]
                if !new_children_seen.contains(&required_col_name) {
                    let colexpr = resolved_col(required_col_name.clone());
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
) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
    // Add aliases to the expressions in the projection to preserve original names when splitting actor pool UDFs.
    // This is needed because when we split actor pool UDFs, we create new names for intermediates, but we would like
    // to have the same expression names as the original projection.
    let aliased_projection_exprs = projection
        .projection
        .iter()
        .map(|expr| {
            if expr.exists(is_actor_pool_udf) && !matches!(expr.as_ref(), Expr::Alias(..)) {
                expr.alias(expr.name())
            } else {
                expr.clone()
            }
        })
        .collect();

    let aliased_projection = Project::try_new(projection.input.clone(), aliased_projection_exprs)?;

    recursive_optimize_project(&aliased_projection, plan, 0)
}

fn recursive_optimize_project(
    projection: &Project,
    plan: Arc<LogicalPlan>,
    recursive_count: usize,
) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
    // TODO: eliminate the need for recursive calls by doing a post-order traversal of the plan tree.

    // Base case: no actor pool UDFs at all
    let has_actor_pool_udfs = projection
        .projection
        .iter()
        .any(|expr| expr.exists(is_actor_pool_udf));
    if !has_actor_pool_udfs {
        return Ok(Transformed::no(plan));
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
        .all(|e| matches!(e.as_ref(), Expr::Column(Column::Resolved(_))))
    {
        // Nothing remaining, we're done splitting and should wire the new node up with the child of the Project
        projection.input.clone()
    } else {
        // Recursively run the rule on the new child Project
        let new_project = Project::try_new(projection.input.clone(), remaining)?;
        let new_child_project = LogicalPlan::Project(new_project.clone()).arced();
        let optimized_child_plan =
            recursive_optimize_project(&new_project, new_child_project, recursive_count + 1)?;
        optimized_child_plan.data
    };

    // Start building a chain of `child -> Project -> ActorPoolProject -> ActorPoolProject -> ... -> Project`
    let (actor_pool_stages, stateless_stages): (Vec<_>, Vec<_>) = truncated_exprs
        .into_iter()
        .partition(|expr| expr.exists(is_actor_pool_udf));

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
                    Some(resolved_col(name))
                }
            })
            .collect_vec()
    };
    let stateless_projection = passthrough_columns
        .into_iter()
        .chain(stateless_stages)
        .collect();
    let new_plan =
        LogicalPlan::Project(Project::try_new(new_plan_child, stateless_projection)?).arced();

    // Iteratively build ActorPoolProject nodes: [...all columns that came before it, actor pool UDF]
    let new_plan = {
        let mut child = new_plan;

        for expr in actor_pool_stages {
            let expr_name = expr.name().to_string();
            let projection = child
                .schema()
                .field_names()
                .filter_map(|name| {
                    if name == expr_name {
                        None
                    } else {
                        Some(resolved_col(name))
                    }
                })
                .chain(iter::once(expr))
                .collect();
            child = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(child, projection)?)
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
            .map(|e| resolved_col(e.name()))
            .collect(),
    )?)
    .arced();

    Ok(Transformed::yes(final_selection_project))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use common_resource_request::ResourceRequest;
    use daft_core::prelude::*;
    use daft_dsl::{
        functions::{
            python::{MaybeInitializedUDF, PythonUDF, RuntimePyObject},
            FunctionExpr,
        },
        resolved_col, Expr, ExprRef,
    };
    use test_log::test;

    use super::SplitActorPoolProjects;
    use crate::{
        ops::{ActorPoolProject, Project},
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::PushDownProjection,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
    };

    /// Helper that creates an optimizer with the SplitExprByActorPoolUDF rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(SplitActorPoolProjects::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    /// Helper that creates an optimizer with the SplitExprByActorPoolUDF rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq_with_projection_pushdown(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![
                    Box::new(SplitActorPoolProjects::new()),
                    Box::new(PushDownProjection::new()),
                ],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    fn create_actor_pool_udf(inputs: Vec<ExprRef>) -> ExprRef {
        Expr::Function {
            func: FunctionExpr::Python(PythonUDF {
                name: Arc::new("foo".to_string()),
                func: MaybeInitializedUDF::Uninitialized {
                    inner: RuntimePyObject::new_none(),
                    init_args: RuntimePyObject::new_none(),
                },
                bound_args: RuntimePyObject::new_none(),
                num_expressions: inputs.len(),
                return_dtype: DataType::Utf8,
                resource_request: Some(create_resource_request()),
                batch_size: None,
                concurrency: Some(8),
            }),
            inputs,
        }
        .arced()
    }

    fn create_resource_request() -> ResourceRequest {
        ResourceRequest::try_new_internal(Some(8.), Some(1.), None).unwrap()
    }

    #[test]
    fn test_with_column_actor_pool_udf_happypath() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let actor_pool_project_expr = create_actor_pool_udf(vec![resolved_col("a")]);

        // Add a Projection with actor pool UDF and resource request
        let project_plan = scan_plan
            .with_columns(vec![actor_pool_project_expr.alias("b")])?
            .build();

        // Project([col("a")]) --> ActorPoolProject([col("a"), foo(col("a")).alias("b")]) --> Project([col("a"), col("b")])
        let expected = scan_plan.select(vec![resolved_col("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![resolved_col("a"), actor_pool_project_expr.alias("b")],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col("a"), resolved_col("b")],
        )?)
        .arced();

        assert_optimized_plan_eq(project_plan, expected)?;

        Ok(())
    }

    #[test]
    fn test_multiple_with_column_parallel() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Utf8),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node(scan_op);
        let project_plan = scan_plan
            .with_columns(vec![
                create_actor_pool_udf(vec![create_actor_pool_udf(vec![resolved_col("a")])])
                    .alias("a_prime"),
                create_actor_pool_udf(vec![create_actor_pool_udf(vec![resolved_col("b")])])
                    .alias("b_prime"),
            ])?
            .build();

        let intermediate_column_name_0 = "__TruncateRootActorPoolUDF_0-2-0__";
        let intermediate_column_name_1 = "__TruncateRootActorPoolUDF_0-3-0__";
        let expected = scan_plan
            .select(vec![resolved_col("a"), resolved_col("b")])?
            .build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                resolved_col("b"),
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_column_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                resolved_col("b"),
                resolved_col(intermediate_column_name_0),
                create_actor_pool_udf(vec![resolved_col("b")]).alias(intermediate_column_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                resolved_col("a"),
                resolved_col("b"),
                resolved_col(intermediate_column_name_0),
                resolved_col(intermediate_column_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                resolved_col(intermediate_column_name_0),
                resolved_col(intermediate_column_name_1),
                resolved_col("a"),
                resolved_col("b"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col(intermediate_column_name_0),
                resolved_col(intermediate_column_name_1),
                resolved_col("a"),
                resolved_col("b"),
                create_actor_pool_udf(vec![resolved_col(intermediate_column_name_0)])
                    .alias("a_prime"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col(intermediate_column_name_0),
                resolved_col(intermediate_column_name_1),
                resolved_col("a"),
                resolved_col("b"),
                resolved_col("a_prime"),
                create_actor_pool_udf(vec![resolved_col(intermediate_column_name_1)])
                    .alias("b_prime"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                resolved_col("a"),
                resolved_col("b"),
                resolved_col("a_prime"),
                resolved_col("b_prime"),
            ],
        )?)
        .arced();
        assert_optimized_plan_eq(project_plan, expected)?;
        Ok(())
    }

    #[test]
    fn test_multiple_with_column_serial() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_actor_pool_project_expr =
            create_actor_pool_udf(vec![create_actor_pool_udf(vec![resolved_col("a")])]);

        // Add a Projection with actor pool UDF and resource request
        // Project([col("a"), foo(foo(col("a"))).alias("b")])
        let project_plan = scan_plan
            .with_columns(vec![stacked_actor_pool_project_expr.alias("b")])?
            .build();

        let intermediate_name = "__TruncateRootActorPoolUDF_0-1-0__";
        let expected = scan_plan.select(vec![resolved_col("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col("a"), resolved_col(intermediate_name)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col(intermediate_name), resolved_col("a")],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col(intermediate_name),
                resolved_col("a"),
                create_actor_pool_udf(vec![resolved_col(intermediate_name)]).alias("b"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col("a"), resolved_col("b")],
        )?)
        .arced();
        assert_optimized_plan_eq(project_plan.clone(), expected)?;

        // With Projection Pushdown, elide intermediate Projects and also perform column pushdown
        let expected = scan_plan.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name),
                resolved_col("a"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                create_actor_pool_udf(vec![resolved_col(intermediate_name)]).alias("b"),
            ],
        )?)
        .arced();
        assert_optimized_plan_eq_with_projection_pushdown(project_plan, expected)?;
        Ok(())
    }

    #[test]
    fn test_multiple_with_column_serial_no_alias() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_actor_pool_project_expr =
            create_actor_pool_udf(vec![create_actor_pool_udf(vec![resolved_col("a")])]);

        // Add a Projection with actor pool UDF and resource request
        let project_plan = scan_plan
            .select(vec![stacked_actor_pool_project_expr])?
            .build();

        let intermediate_name = "__TruncateRootActorPoolUDF_0-0-0__";

        let expected = scan_plan.select(vec![resolved_col("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col(intermediate_name)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col(intermediate_name)],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col(intermediate_name),
                create_actor_pool_udf(vec![resolved_col(intermediate_name)]).alias("a"),
            ],
        )?)
        .arced();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![resolved_col("a")])?).arced();
        assert_optimized_plan_eq(project_plan.clone(), expected)?;

        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            scan_plan.build(),
            vec![create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name)],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![create_actor_pool_udf(vec![resolved_col(intermediate_name)]).alias("a")],
        )?)
        .arced();
        assert_optimized_plan_eq_with_projection_pushdown(project_plan, expected)?;

        Ok(())
    }

    #[test]
    fn test_multiple_with_column_serial_multiarg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Utf8),
            Field::new("b", DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_actor_pool_project_expr = create_actor_pool_udf(vec![
            create_actor_pool_udf(vec![resolved_col("a")]),
            create_actor_pool_udf(vec![resolved_col("b")]),
        ]);

        // Add a Projection with actor pool UDF and resource request
        // Project([foo(foo(col("a")), foo(col("b"))).alias("c")])
        let project_plan = scan_plan
            .select(vec![stacked_actor_pool_project_expr.alias("c")])?
            .build();

        let intermediate_name_0 = "__TruncateRootActorPoolUDF_0-0-0__";
        let intermediate_name_1 = "__TruncateRootActorPoolUDF_0-0-1__";
        let expected = scan_plan
            .select(vec![resolved_col("a"), resolved_col("b")])?
            .build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                resolved_col("b"),
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                resolved_col("b"),
                resolved_col(intermediate_name_0),
                create_actor_pool_udf(vec![resolved_col("b")]).alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_0),
                resolved_col(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_0),
                resolved_col(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_0),
                resolved_col(intermediate_name_1),
                create_actor_pool_udf(vec![
                    resolved_col(intermediate_name_0),
                    resolved_col(intermediate_name_1),
                ])
                .alias("c"),
            ],
        )?)
        .arced();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![resolved_col("c")])?).arced();
        assert_optimized_plan_eq(project_plan.clone(), expected)?;

        // With Projection Pushdown, elide intermediate Projects and also perform column pushdown
        let expected = scan_plan.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name_0),
                resolved_col("b"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_0),
                create_actor_pool_udf(vec![resolved_col("b")]).alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![create_actor_pool_udf(vec![
                resolved_col(intermediate_name_0),
                resolved_col(intermediate_name_1),
            ])
            .alias("c")],
        )?)
        .arced();
        assert_optimized_plan_eq_with_projection_pushdown(project_plan, expected)?;
        Ok(())
    }

    #[test]
    fn test_multiple_with_column_serial_multiarg_with_intermediate_stateless() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_actor_pool_project_expr =
            create_actor_pool_udf(vec![create_actor_pool_udf(vec![resolved_col("a")])
                .add(create_actor_pool_udf(vec![resolved_col("b")]))]);

        // Add a Projection with actor pool UDF and resource request
        // Project([foo(foo(col("a")) + foo(col("b"))).alias("c")])
        let project_plan = scan_plan
            .select(vec![stacked_actor_pool_project_expr.alias("c")])?
            .build();

        let intermediate_name_0 = "__TruncateAnyActorPoolUDFChildren_1-0-0__";
        let intermediate_name_1 = "__TruncateAnyActorPoolUDFChildren_1-0-1__";
        let intermediate_name_2 = "__TruncateRootActorPoolUDF_0-0-0__";
        let expected = scan_plan
            .select(vec![resolved_col("a"), resolved_col("b")])?
            .build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                resolved_col("b"),
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                resolved_col("b"),
                resolved_col(intermediate_name_0),
                create_actor_pool_udf(vec![resolved_col("b")]).alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_0),
                resolved_col(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_0),
                resolved_col(intermediate_name_1),
                resolved_col(intermediate_name_0)
                    .add(resolved_col(intermediate_name_1))
                    .alias(intermediate_name_2),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col(intermediate_name_2)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col(intermediate_name_2)],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_2),
                create_actor_pool_udf(vec![resolved_col(intermediate_name_2)]).alias("c"),
            ],
        )?)
        .arced();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![resolved_col("c")])?).arced();
        assert_optimized_plan_eq(project_plan.clone(), expected)?;

        // With Projection Pushdown, elide intermediate Projects and also perform column pushdown
        let expected = scan_plan.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name_0),
                resolved_col("b"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_0),
                create_actor_pool_udf(vec![resolved_col("b")]).alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col(intermediate_name_0)
                .add(resolved_col(intermediate_name_1))
                .alias(intermediate_name_2)],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![create_actor_pool_udf(vec![resolved_col(intermediate_name_2)]).alias("c")],
        )?)
        .arced();
        assert_optimized_plan_eq_with_projection_pushdown(project_plan, expected)?;
        Ok(())
    }

    #[test]
    fn test_nested_with_column_same_names() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Int64)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_actor_pool_project_expr = create_actor_pool_udf(vec![
            resolved_col("a").add(create_actor_pool_udf(vec![resolved_col("a")]))
        ]);

        // Add a Projection with actor pool UDF and resource request
        // Project([foo(col("a") + foo(col("a"))).alias("c")])
        let project_plan = scan_plan
            .select(vec![
                resolved_col("a"),
                stacked_actor_pool_project_expr.alias("c"),
            ])?
            .build();

        let intermediate_name_0 = "__TruncateAnyActorPoolUDFChildren_1-1-0__";
        let intermediate_name_1 = "__TruncateRootActorPoolUDF_0-1-0__";
        let expected = scan_plan.build();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![resolved_col("a")])?).arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col("a"), resolved_col(intermediate_name_0)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_0),
                resolved_col("a"),
                resolved_col("a")
                    .add(resolved_col(intermediate_name_0))
                    .alias(intermediate_name_1),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col("a"), resolved_col(intermediate_name_1)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col(intermediate_name_1), resolved_col("a")],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_1),
                resolved_col("a"),
                create_actor_pool_udf(vec![resolved_col(intermediate_name_1)]).alias("c"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col("a"), resolved_col("c")],
        )?)
        .arced();

        assert_optimized_plan_eq(project_plan, expected)?;

        Ok(())
    }

    #[test]
    fn test_stateless_expr_with_only_some_actor_pool_children() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Int64)]);
        let scan_plan = dummy_scan_node(scan_op);

        // (col("a") + col("a"))  +  foo(col("a"))
        let actor_pool_project_expr = resolved_col("a")
            .add(resolved_col("a"))
            .add(create_actor_pool_udf(vec![resolved_col("a")]))
            .alias("result");
        let project_plan = scan_plan
            .select(vec![resolved_col("a"), actor_pool_project_expr])?
            .build();

        let intermediate_name_0 = "__TruncateAnyActorPoolUDFChildren_0-1-0__";
        // let intermediate_name_1 = "__TruncateRootActorPoolUDF_0-1-0__";
        let expected = scan_plan.build();
        let expected =
            LogicalPlan::Project(Project::try_new(expected, vec![resolved_col("a")])?).arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                resolved_col("a"),
                create_actor_pool_udf(vec![resolved_col("a")]).alias(intermediate_name_0),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col("a"), resolved_col(intermediate_name_0)],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                resolved_col(intermediate_name_0),
                resolved_col("a"),
                resolved_col("a")
                    .add(resolved_col("a"))
                    .add(resolved_col(intermediate_name_0))
                    .alias("result"),
            ],
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![resolved_col("a"), resolved_col("result")],
        )?)
        .arced();

        assert_optimized_plan_eq(project_plan, expected)?;

        Ok(())
    }
}
