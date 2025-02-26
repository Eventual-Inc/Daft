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
            Expr::Alias(inner, _) => Self::is_monotonic_id_expr(inner),
            _ => false,
        }
    }

    /// Helper function to find all monotonically_increasing_id() expressions in a Project operation
    /// Returns a vector of (column_name, original_expr) pairs for each monotonic ID found
    fn find_monotonic_ids(project: &Project) -> Vec<(String, ExprRef)> {
        let mut result = Vec::new();

        for expr in &project.projection {
            if Self::is_monotonic_id_expr(expr) {
                let column_name = match expr.as_ref() {
                    Expr::Alias(_, name) => name.to_string(),
                    _ => "id".to_string(), // Default name if not aliased
                };
                result.push((column_name, expr.clone()));
            }
        }

        result
    }

    /// Helper function to create a chain of MonotonicallyIncreasingId operations
    fn create_monotonic_chain(
        input: Arc<LogicalPlan>,
        monotonic_ids: Vec<(String, ExprRef)>,
    ) -> DaftResult<Arc<LogicalPlan>> {
        let mut current_plan = input;

        // Create a chain of MonotonicallyIncreasingId operations
        for (column_name, _) in monotonic_ids {
            current_plan = Arc::new(LogicalPlan::MonotonicallyIncreasingId(
                MonotonicallyIncreasingId::try_new(current_plan, Some(&column_name))?,
            ));
        }

        Ok(current_plan)
    }

    /// Helper function to create a new projection list that preserves the original order
    /// and replaces monotonically_increasing_id() calls with column references
    fn create_new_projection(
        original_projection: &[ExprRef],
        _monotonic_ids: &[(String, ExprRef)],
    ) -> Vec<ExprRef> {
        original_projection
            .iter()
            .map(|expr| {
                if Self::is_monotonic_id_expr(expr) {
                    // Find the corresponding column name for this monotonic ID
                    let column_name = match expr.as_ref() {
                        Expr::Alias(_, name) => name.clone(),
                        _ => Arc::from("id"),
                    };
                    // Replace with a column reference
                    Expr::Column(Column::Resolved(ResolvedColumn::Basic(column_name))).into()
                } else {
                    expr.clone()
                }
            })
            .collect()
    }
}

impl OptimizerRule for DetectMonotonicId {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| {
            match node.as_ref() {
                LogicalPlan::Project(project) => {
                    // Find all monotonically_increasing_id() calls in this Project
                    let monotonic_ids = Self::find_monotonic_ids(project);

                    if !monotonic_ids.is_empty() {
                        // Create a chain of MonotonicallyIncreasingId operations
                        let monotonic_plan = Self::create_monotonic_chain(
                            project.input.clone(),
                            monotonic_ids.clone(),
                        )?;

                        // Create a new projection list that preserves the original order
                        let new_projection =
                            Self::create_new_projection(&project.projection, &monotonic_ids);

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
