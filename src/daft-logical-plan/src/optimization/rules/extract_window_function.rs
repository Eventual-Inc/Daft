use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{expr::window::WindowSpec, functions::FunctionExpr, resolved_col, Expr, ExprRef};

use crate::{
    logical_plan::{LogicalPlan, Project},
    ops::Window,
    optimization::rules::OptimizerRule,
};

/// Optimization rule that detects window function expressions (e.g., sum().over(window))
/// and transforms them into Window operations.
#[derive(Debug)]
pub struct ExtractWindowFunction;

impl Default for ExtractWindowFunction {
    fn default() -> Self {
        Self
    }
}

impl ExtractWindowFunction {
    /// Creates a new instance of ExtractWindowFunction
    pub fn new() -> Self {
        Self
    }

    /// Helper function to detect if an expression is a window function call (i.e., has .over())
    fn is_window_function_expr(expr: &ExprRef) -> bool {
        let result = match expr.as_ref() {
            // Check if this is a function expression with a window function evaluator
            Expr::Function { func, .. } => {
                let is_window = matches!(func, FunctionExpr::Window(_));
                is_window
            }
            // Recursively check children
            _ => expr.children().iter().any(Self::is_window_function_expr),
        };
        result
    }

    /// Helper function to check if any expression in the projection contains window functions
    fn contains_window_function(project: &Project) -> bool {
        project.projection.iter().any(Self::is_window_function_expr)
    }

    /// Helper function to extract window function expressions from a projection
    fn extract_window_functions(projection: &[ExprRef]) -> Vec<(ExprRef, WindowSpec)> {
        let mut result = Vec::new();

        for expr in projection {
            Self::collect_window_functions(expr, &mut result);
        }

        result
    }

    /// Helper function to recursively collect window functions from an expression
    fn collect_window_functions(expr: &ExprRef, result: &mut Vec<(ExprRef, WindowSpec)>) {
        match expr.as_ref() {
            Expr::Function { func, .. } => {
                // If this is a window function, extract its window spec
                if let FunctionExpr::Window(window_func) = func {
                    result.push((expr.clone(), window_func.window_spec.clone()));
                }
            }
            // Recursively check children
            _ => {
                for child in expr.children() {
                    Self::collect_window_functions(&child, result);
                }
            }
        }
    }

    /// Helper function to replace window function expressions with column references to the Window output
    fn replace_window_functions(
        expr: &ExprRef,
        window_col_mappings: &[(ExprRef, String)],
    ) -> DaftResult<ExprRef> {
        // Use transform pattern similar to replace_monotonic_id in DetectMonotonicId
        let transformed = expr.clone().transform(|e| {
            // First, check if this expression is a window function that needs to be replaced
            for (window_expr, col_name) in window_col_mappings {
                if Arc::ptr_eq(&e, window_expr) {
                    // Replace with a column reference and mark as transformed
                    return Ok(Transformed::yes(resolved_col(col_name.clone())));
                }
            }
            // Not a window function to replace directly
            Ok(Transformed::no(e))
        })?;

        Ok(transformed.data)
    }
}

impl OptimizerRule for ExtractWindowFunction {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| {
            match node.as_ref() {
                LogicalPlan::Project(project) => {
                    // Check if any expression contains window functions
                    if Self::contains_window_function(project) {
                        // Extract window functions and their specs
                        let window_funcs = Self::extract_window_functions(&project.projection);

                        if !window_funcs.is_empty() {
                            let sample_window_spec = &window_funcs[0].1;

                            // Extract the window function expressions
                            let window_function_exprs = window_funcs
                                .iter()
                                .map(|(expr, _)| expr.clone())
                                .collect::<Vec<ExprRef>>();

                            // Create a Window operation with the window functions
                            let window_plan = Arc::new(LogicalPlan::Window(
                                Window::try_new(
                                    project.input.clone(),
                                    window_function_exprs.clone(),
                                    sample_window_spec.clone(),
                                )?
                                .with_window_functions(window_function_exprs),
                            ));

                            // Create mappings from window function expressions to column names in the Window output
                            let window_col_mappings: Vec<(ExprRef, String)> = window_funcs
                                .iter()
                                .enumerate()
                                .map(|(i, (expr, _))| (expr.clone(), format!("window_{}", i)))
                                .collect();

                            // Replace window function expressions with column references in the projection
                            let new_projection = project
                                .projection
                                .iter()
                                .map(|expr| {
                                    Self::replace_window_functions(expr, &window_col_mappings)
                                })
                                .collect::<DaftResult<Vec<ExprRef>>>()?;

                            // Create a new Project operation with the updated projection list
                            let final_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                                window_plan,
                                new_projection,
                            )?));

                            Ok(Transformed::yes(final_plan))
                        } else {
                            Ok(Transformed::no(node))
                        }
                    } else {
                        Ok(Transformed::no(node))
                    }
                }
                _ => Ok(Transformed::no(node)),
            }
        })
    }
}
