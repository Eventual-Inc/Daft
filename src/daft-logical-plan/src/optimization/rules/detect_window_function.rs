use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{
    expr::window::WindowSpec,
    Expr, ExprRef, functions::FunctionExpr, resolved_col,
};

use crate::{
    logical_plan::{LogicalPlan, Project},
    ops::Window,
    optimization::rules::OptimizerRule,
};

/// Optimization rule that detects window function expressions (e.g., sum().over(window))
/// and transforms them into Window operations.
#[derive(Debug)]
pub struct DetectWindowFunction;

impl Default for DetectWindowFunction {
    fn default() -> Self {
        Self
    }
}

impl DetectWindowFunction {
    /// Creates a new instance of DetectWindowFunction
    pub fn new() -> Self {
        Self
    }

    /// Helper function to detect if an expression is a window function call (i.e., has .over())
    fn is_window_function_expr(expr: &ExprRef) -> bool {
        let result = match expr.as_ref() {
            // Check if this is a function expression with a window function evaluator
            Expr::Function { func, .. } => {
                let is_window = matches!(func, FunctionExpr::Window(_));
                if is_window {
                    println!("DetectWindowFunction: Found window function: {:?}", expr);
                }
                is_window
            }
            // Recursively check children
            _ => expr.children().iter().any(Self::is_window_function_expr),
        };
        result
    }

    /// Helper function to check if any expression in the projection contains window functions
    fn contains_window_function(project: &Project) -> bool {
        let contains = project.projection.iter().any(Self::is_window_function_expr);
        println!("DetectWindowFunction: Project contains window functions: {}", contains);
        contains
    }

    /// Helper function to extract window function expressions from a projection
    fn extract_window_functions(projection: &[ExprRef]) -> Vec<(ExprRef, WindowSpec)> {
        let mut result = Vec::new();
        
        for expr in projection {
            Self::collect_window_functions(expr, &mut result);
        }
        
        println!("DetectWindowFunction: Extracted {} window functions", result.len());
        if !result.is_empty() {
            for (i, (expr, spec)) in result.iter().enumerate() {
                println!("DetectWindowFunction: Window function {}: {:?} with spec {:?}", i, expr, spec);
            }
        }
        result
    }
    
    /// Helper function to recursively collect window functions from an expression
    fn collect_window_functions(expr: &ExprRef, result: &mut Vec<(ExprRef, WindowSpec)>) {
        match expr.as_ref() {
            Expr::Function { func, .. } => {
                // If this is a window function, extract its window spec
                if let FunctionExpr::Window(window_func) = func {
                    println!("DetectWindowFunction: Collecting window function: {:?} with spec {:?}", expr, window_func.window_spec);
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
        println!("DetectWindowFunction: Replacing window functions in expression: {:?}", expr);
        // Use transform pattern similar to replace_monotonic_id in DetectMonotonicId
        let transformed = expr.clone().transform(|e| {
            // First, check if this expression is a window function that needs to be replaced
            for (window_expr, col_name) in window_col_mappings {
                if Arc::ptr_eq(&e, window_expr) {
                    // Replace with a column reference and mark as transformed
                    println!("DetectWindowFunction: Replacing window function {:?} with column reference to {}", e, col_name);
                    return Ok(Transformed::yes(resolved_col(col_name.clone()).into()));
                }
            }
            // Not a window function to replace directly
            Ok(Transformed::no(e))
        })?;
        
        Ok(transformed.data)
    }
}

impl OptimizerRule for DetectWindowFunction {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        println!("DetectWindowFunction: Attempting to optimize plan: {:?}", plan);
        plan.transform_down(|node| {
            match node.as_ref() {
                LogicalPlan::Project(project) => {
                    println!("DetectWindowFunction: Inspecting Project operation: {:?}", project);
                    // Check if any expression contains window functions
                    if Self::contains_window_function(&project) {
                        // Extract window functions and their specs
                        let window_funcs = Self::extract_window_functions(&project.projection);
                        
                        // Group window functions by their window specs
                        // This would allow us to create a single Window operation for each unique window spec
                        
                        // For simplicity in this implementation, assume all window functions share the same spec
                        // In a real implementation, we would group by window spec
                        
                        if !window_funcs.is_empty() {
                            let sample_window_spec = &window_funcs[0].1;
                            println!("DetectWindowFunction: Using window spec: {:?}", sample_window_spec);
                            
                            // Extract the window function expressions
                            let window_function_exprs = window_funcs.iter()
                                .map(|(expr, _)| expr.clone())
                                .collect::<Vec<ExprRef>>();
                            
                            println!("DetectWindowFunction: Creating Window operation with {} window functions", window_function_exprs.len());
                            // Create a Window operation with the window functions
                            let window_plan = Arc::new(LogicalPlan::Window(
                                Window::try_new(
                                    project.input.clone(),
                                    window_function_exprs.clone(),
                                    sample_window_spec.partition_by.clone(),
                                    sample_window_spec.order_by.clone(),
                                    vec![true; sample_window_spec.order_by.len()],
                                    sample_window_spec.frame.clone(),
                                )?
                                .with_window_functions(window_function_exprs),
                            ));
                            
                            // Create mappings from window function expressions to column names in the Window output
                            let window_col_mappings: Vec<(ExprRef, String)> = window_funcs
                                .iter()
                                .enumerate()
                                .map(|(i, (expr, _))| (expr.clone(), format!("window_{}", i)))
                                .collect();
                            
                            println!("DetectWindowFunction: Created {} window column mappings", window_col_mappings.len());
                            for (i, (expr, col_name)) in window_col_mappings.iter().enumerate() {
                                println!("DetectWindowFunction: Mapping {} - {:?} -> {}", i, expr, col_name);
                            }
                            
                            // Replace window function expressions with column references in the projection
                            let new_projection = project.projection
                                .iter()
                                .map(|expr| Self::replace_window_functions(expr, &window_col_mappings))
                                .collect::<DaftResult<Vec<ExprRef>>>()?;
                            
                            println!("DetectWindowFunction: Created new projection with {} expressions", new_projection.len());
                            
                            // Create a new Project operation with the updated projection list
                            let final_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                                window_plan,
                                new_projection,
                            )?));
                            
                            println!("DetectWindowFunction: Successfully transformed the plan with Window operation");
                            Ok(Transformed::yes(final_plan))
                        } else {
                            println!("DetectWindowFunction: No window functions found, skipping transformation");
                            Ok(Transformed::no(node))
                        }
                    } else {
                        println!("DetectWindowFunction: No window functions in this Project operation");
                        Ok(Transformed::no(node))
                    }
                }
                _ => {
                    println!("DetectWindowFunction: Skipping non-Project operation: {:?}", node);
                    Ok(Transformed::no(node))
                },
            }
        })
    }
} 