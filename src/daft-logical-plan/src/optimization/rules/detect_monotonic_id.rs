use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{Column, Expr, ExprRef, ResolvedColumn};

use crate::{
    logical_plan::{LogicalPlan, Project},
    ops::MonotonicallyIncreasingId,
    optimization::rules::OptimizerRule,
};

/// Optimization rule that detects monotonically_increasing_id() expressions in Project operations
/// and transforms them into MonotonicallyIncreasingId operations.
#[derive(Debug)]
pub struct DetectMonotonicId;

impl Default for DetectMonotonicId {
    fn default() -> Self {
        Self
    }
}

impl DetectMonotonicId {
    /// Creates a new instance of DetectMonotonicId
    pub fn new() -> Self {
        Self
    }

    /// Helper function to detect if an expression is a monotonically_increasing_id() call
    fn is_monotonic_id_expr(expr: &ExprRef) -> bool {
        match expr.as_ref() {
            Expr::ScalarFunction(func) => func.name() == "monotonically_increasing_id",
            _ => expr.children().iter().any(Self::is_monotonic_id_expr),
        }
    }

    /// Helper function to check if any expression in the projection contains monotonically_increasing_id()
    fn contains_monotonic_id(project: &Project) -> bool {
        project.projection.iter().any(Self::is_monotonic_id_expr)
    }

    /// Helper function to replace monotonically_increasing_id() expressions with column references
    fn replace_monotonic_id(expr: &ExprRef, column_name: &str) -> DaftResult<ExprRef> {
        Ok(expr
            .clone()
            .transform(|e| match e.as_ref() {
                Expr::ScalarFunction(func) if func.name() == "monotonically_increasing_id" => {
                    Ok(Transformed::yes(
                        Expr::Column(Column::Resolved(ResolvedColumn::Basic(Arc::from(
                            column_name,
                        ))))
                        .into(),
                    ))
                }
                _ => Ok(Transformed::no(e)),
            })?
            .data)
    }

    /// Helper function to replace all monotonically_increasing_id() expressions with column references
    fn replace_monotonic_id_expressions(
        projection: &[ExprRef],
        column_name: &str,
    ) -> DaftResult<Vec<ExprRef>> {
        projection
            .iter()
            .map(|expr| Self::replace_monotonic_id(expr, column_name))
            .collect()
    }
}

impl OptimizerRule for DetectMonotonicId {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| {
            match node.as_ref() {
                LogicalPlan::Project(project) => {
                    // Check if any expression contains monotonically_increasing_id()
                    if Self::contains_monotonic_id(project) {
                        // Use a fixed column name for the monotonic ID
                        let column_name = "id";

                        // Create a single MonotonicallyIncreasingId operation
                        let monotonic_plan = Arc::new(LogicalPlan::MonotonicallyIncreasingId(
                            MonotonicallyIncreasingId::try_new(
                                project.input.clone(),
                                Some(column_name),
                            )?,
                        ));

                        // Replace all monotonically_increasing_id() expressions with column references
                        let new_projection = Self::replace_monotonic_id_expressions(
                            &project.projection,
                            column_name,
                        )?;

                        // Create a new Project operation with the updated projection list
                        let final_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                            monotonic_plan,
                            new_projection,
                        )?));

                        Ok(Transformed::yes(final_plan))
                    } else {
                        Ok(Transformed::no(node))
                    }
                }
                _ => Ok(Transformed::no(node)),
            }
        })
    }
}
