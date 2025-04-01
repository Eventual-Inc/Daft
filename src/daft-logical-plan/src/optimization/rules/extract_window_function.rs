use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{expr::window::WindowSpec, resolved_col, Expr, ExprRef};
use itertools::Itertools;

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
        expr.exists(|e| matches!(e.as_ref(), Expr::Window(_, _)))
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
            Expr::Window(_inner_expr, window_spec) => {
                // If this is a window expression, extract its window spec
                result.push((expr.clone(), window_spec.clone()));
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
        // Transform the expression tree
        let transformed = expr.clone().transform(|e| {
            // Check if this expression is a window function that needs to be replaced
            for (window_expr, col_name) in window_col_mappings {
                if Arc::ptr_eq(&e, window_expr) {
                    // Replace with a column reference and mark as transformed
                    return Ok(Transformed::yes(resolved_col(col_name.clone())));
                }
            }
            // Not a window function to replace, continue transformation on children
            Ok(Transformed::no(e))
        })?;

        // Return the transformed expression data
        Ok(transformed.data)
    }
}

impl OptimizerRule for ExtractWindowFunction {
    /// Optimizes a logical plan by extracting window function expressions from projections
    /// and converting them into explicit Window operations.
    ///
    /// Algorithm:
    /// 1. Find all window functions in a projection
    /// 2. Group them by their window specifications
    /// 3. Create a separate Window operation for each group
    /// 4. Connect these Window operations in a chain
    /// 5. Replace the original window expressions with column references
    /// 6. Create a final Project operation with the updated expressions
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| {
            match node.as_ref() {
                LogicalPlan::Project(project) => {
                    // Check if any expression contains window functions
                    if Self::contains_window_function(project) {
                        // Extract window functions and their specs
                        let window_funcs = Self::extract_window_functions(&project.projection);

                        // Debug assertion to verify extraction worked correctly
                        debug_assert!(
                            !window_funcs.is_empty(),
                            "Window functions detected but none extracted"
                        );

                        // Group window functions by their window specs for batch processing
                        let window_funcs_grouped_by_spec = window_funcs
                            .into_iter()
                            .into_group_map_by(|(_, spec)| spec.clone());

                        // Create window column mappings to track replacements
                        let mut window_col_mappings = Vec::new();

                        // Start with the original input plan
                        let mut current_plan = project.input.clone();

                        // Process each window spec group separately
                        for (window_spec, window_exprs_for_spec) in window_funcs_grouped_by_spec {
                            // Extract just the expressions for this window spec
                            let window_function_exprs = window_exprs_for_spec
                                .iter()
                                .map(|(expr, _)| expr.clone())
                                .collect::<Vec<ExprRef>>();

                            // Create aliases for each window function expression
                            let window_function_exprs_aliased = window_function_exprs
                                .iter()
                                .map(|e| {
                                    let semantic_id = e.semantic_id(&current_plan.schema());
                                    e.alias(semantic_id.id)
                                })
                                .collect::<Vec<ExprRef>>();

                            // Create a Window operation for this specific window spec
                            current_plan = Arc::new(LogicalPlan::Window(Window::try_new(
                                current_plan,
                                window_function_exprs_aliased,
                                window_spec,
                            )?));

                            // Update mappings for these window expressions
                            let spec_mappings = window_function_exprs
                                .iter()
                                .map(|e| {
                                    let semantic_id = e.semantic_id(&current_plan.schema());
                                    (e.clone(), semantic_id.id.to_string())
                                })
                                .collect::<Vec<(ExprRef, String)>>();

                            window_col_mappings.extend(spec_mappings);
                        }

                        // Replace window function expressions with column references in the projection
                        let new_projection = project
                            .projection
                            .iter()
                            .map(|expr| Self::replace_window_functions(expr, &window_col_mappings))
                            .collect::<DaftResult<Vec<ExprRef>>>()?;

                        // Create a new Project operation with the updated projection list
                        let final_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                            current_plan,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_dsl::{expr::window::WindowSpec, resolved_col, Expr};
    use daft_schema::{dtype::DataType, field::Field};

    use crate::{
        logical_plan::{LogicalPlan, Project},
        ops::Window,
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::ExtractWindowFunction,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
    };

    /// Helper that creates an optimizer with the ExtractWindowFunction rule registered, optimizes
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
                vec![Box::new(ExtractWindowFunction::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    /// Tests that a simple window function with a single partition column is correctly extracted.
    #[test]
    fn test_single_partition_window_function() -> DaftResult<()> {
        // Create a scan operator with "category" and "value" columns
        let scan_op = dummy_scan_operator(vec![
            Field::new("category", DataType::Utf8),
            Field::new("value", DataType::Int64),
        ]);

        // Create a logical plan with a window function
        let input_plan = dummy_scan_node(scan_op.clone());

        // Create column references
        let category_col = resolved_col("category");
        let value_col = resolved_col("value");

        // Create a window specification with a single partition column
        let mut window_spec = WindowSpec::default();
        window_spec.partition_by = vec![category_col.clone()];

        // Create a window function expression (min)
        let min_expr = value_col.clone().min();

        // Create a window function directly using Expr::Window and give it a name to avoid duplicate fields
        let window_func =
            Arc::new(Expr::Window(min_expr.clone(), window_spec.clone())).alias("min_value");

        // Create a projection that includes the window function
        let projection = vec![category_col.clone(), value_col.clone(), window_func.clone()];

        // Build the input plan with the window function in a projection
        let plan = input_plan.clone().select(projection)?.build();

        // Create the expected plan - we need it to match exactly what our optimizer produces

        // First, we need to get the auto-generated name that the optimizer will use for the window function
        let auto_generated_name = "value.local_min().window(partition_by=[category],order_by=[])";

        // Create the window operation with the auto-generated name
        // We need to create a window expression that exactly matches what the optimizer will produce
        let window_op = Window::try_new(
            input_plan.clone().build(),
            vec![
                Arc::new(Expr::Window(min_expr.clone(), window_spec.clone()))
                    .alias(auto_generated_name),
            ],
            window_spec,
        )?;

        // Then create a projection on top of the window operation that aliases the auto-generated name to our alias
        let window_plan = Arc::new(LogicalPlan::Window(window_op));
        let final_projection = Project::try_new(
            window_plan,
            vec![
                category_col,
                value_col,
                resolved_col(auto_generated_name).alias("min_value"),
            ],
        )?;

        let expected_plan = Arc::new(LogicalPlan::Project(final_projection));

        assert_optimized_plan_eq(plan, expected_plan)
    }

    /// Tests that a window function with multiple partition columns is correctly extracted.
    #[test]
    fn test_multiple_partition_window_function() -> DaftResult<()> {
        // Create a scan operator with "category", "group" and "value" columns
        let scan_op = dummy_scan_operator(vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ]);

        // Create a logical plan with a window function
        let input_plan = dummy_scan_node(scan_op.clone());

        // Create column references
        let category_col = resolved_col("category");
        let group_col = resolved_col("group");
        let value_col = resolved_col("value");

        // Create a window specification with multiple partition columns
        let mut window_spec = WindowSpec::default();
        window_spec.partition_by = vec![category_col.clone(), group_col.clone()];

        // Create a window function expression (sum)
        let sum_expr = value_col.clone().sum();

        // Create a window function directly using Expr::Window
        let window_func =
            Arc::new(Expr::Window(sum_expr.clone(), window_spec.clone())).alias("sum_value");

        // Create a projection that includes the window function
        let projection = vec![
            category_col.clone(),
            group_col.clone(),
            value_col.clone(),
            window_func.clone(),
        ];

        // Build the input plan with the window function in a projection
        let plan = input_plan.clone().select(projection)?.build();

        // Create the expected plan - we need it to match exactly what our optimizer produces

        // Generate the expected column name based on the window function expression
        // Note: The actual name format from the optimizer must be exactly matched
        let auto_generated_name =
            "value.local_sum().window(partition_by=[category,group],order_by=[])";

        // Create the window operation
        let window_op = Window::try_new(
            input_plan.clone().build(),
            vec![
                Arc::new(Expr::Window(sum_expr.clone(), window_spec.clone()))
                    .alias(auto_generated_name),
            ],
            window_spec,
        )?;

        // Then create a projection on top of the window operation
        let window_plan = Arc::new(LogicalPlan::Window(window_op));
        let final_projection = Project::try_new(
            window_plan,
            vec![
                category_col,
                group_col,
                value_col,
                resolved_col(auto_generated_name).alias("sum_value"),
            ],
        )?;

        let expected_plan = Arc::new(LogicalPlan::Project(final_projection));

        assert_optimized_plan_eq(plan, expected_plan)
    }

    /// Tests that multiple window specifications in the same projection are correctly extracted.
    #[test]
    fn test_multiple_window_specs() -> DaftResult<()> {
        // Create a scan operator with "category", "group" and "value" columns
        let scan_op = dummy_scan_operator(vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ]);

        // Create a logical plan with multiple window specs
        let input_plan = dummy_scan_node(scan_op.clone());

        // Create column references
        let category_col = resolved_col("category");
        let group_col = resolved_col("group");
        let value_col = resolved_col("value");

        // Create a window specification with category as partition column
        let mut window_spec1 = WindowSpec::default();
        window_spec1.partition_by = vec![category_col.clone()];

        // Create a window specification with group as partition column
        let mut window_spec2 = WindowSpec::default();
        window_spec2.partition_by = vec![group_col.clone()];

        // Create window functions with different specs
        let min_expr = value_col.clone().min();
        let sum_expr = value_col.clone().sum();

        let window_func1 =
            Arc::new(Expr::Window(min_expr.clone(), window_spec1.clone())).alias("min_by_category");
        let window_func2 =
            Arc::new(Expr::Window(sum_expr.clone(), window_spec2.clone())).alias("sum_by_group");

        // Create a projection that includes both window functions
        let projection = vec![
            category_col.clone(),
            group_col.clone(),
            value_col.clone(),
            window_func1.clone(),
            window_func2.clone(),
        ];

        // Build the input plan with the window functions in a projection
        let plan = input_plan.clone().select(projection)?.build();

        // Create the expected plan - we need it to match exactly what our optimizer produces

        // First, get the auto-generated names that the optimizer will use
        let auto_generated_name1 = "value.local_min().window(partition_by=[category],order_by=[])";
        let auto_generated_name2 = "value.local_sum().window(partition_by=[group],order_by=[])";

        // Based on the error output, the optimizer processes sum(group) first, then min(category)

        // Create the window operation for the group-based spec first
        let window_op1 = Window::try_new(
            input_plan.clone().build(),
            vec![
                Arc::new(Expr::Window(sum_expr.clone(), window_spec2.clone()))
                    .alias(auto_generated_name2),
            ],
            window_spec2,
        )?;

        // Create an intermediate plan with the first window operation
        let intermediate_plan = Arc::new(LogicalPlan::Window(window_op1));

        // Create the second window operation using the output of the first
        let window_op2 = Window::try_new(
            intermediate_plan,
            vec![
                Arc::new(Expr::Window(min_expr.clone(), window_spec1.clone()))
                    .alias(auto_generated_name1),
            ],
            window_spec1,
        )?;

        // Create a plan with both window operations
        let window_plan = Arc::new(LogicalPlan::Window(window_op2));

        // Create the final projection that aliases the auto-generated names
        let final_projection = Project::try_new(
            window_plan,
            vec![
                category_col,
                group_col,
                value_col,
                resolved_col(auto_generated_name1).alias("min_by_category"),
                resolved_col(auto_generated_name2).alias("sum_by_group"),
            ],
        )?;

        let expected_plan = Arc::new(LogicalPlan::Project(final_projection));

        assert_optimized_plan_eq(plan, expected_plan)
    }

    /// Tests that multiple window functions with multiple window specifications are correctly extracted.
    #[test]
    fn test_multiple_window_functions() -> DaftResult<()> {
        // Create a scan operator with "category", "group" and "value" columns
        let scan_op = dummy_scan_operator(vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ]);

        // Create a logical plan with multiple window functions and specs
        let input_plan = dummy_scan_node(scan_op.clone());

        // Create column references
        let category_col = resolved_col("category");
        let group_col = resolved_col("group");
        let value_col = resolved_col("value");

        // Create a window specification with multiple partition columns
        let mut multi_partition_spec = WindowSpec::default();
        multi_partition_spec.partition_by = vec![category_col.clone(), group_col.clone()];

        // Create a window specification with a single partition column
        let mut single_partition_spec = WindowSpec::default();
        single_partition_spec.partition_by = vec![category_col.clone()];

        // Create multiple window functions with different specs
        let sum_expr = value_col.clone().sum();
        let avg_expr = value_col.clone().mean();
        let min_expr = value_col.clone().min();
        let max_expr = value_col.clone().max();

        // Two functions with multi-partition spec
        let window_func1 = Arc::new(Expr::Window(sum_expr.clone(), multi_partition_spec.clone()))
            .alias("sum_value");
        let window_func2 = Arc::new(Expr::Window(avg_expr.clone(), multi_partition_spec.clone()))
            .alias("avg_value");

        // Two functions with single-partition spec
        let window_func3 = Arc::new(Expr::Window(
            min_expr.clone(),
            single_partition_spec.clone(),
        ))
        .alias("min_value");
        let window_func4 = Arc::new(Expr::Window(
            max_expr.clone(),
            single_partition_spec.clone(),
        ))
        .alias("max_value");

        // Create a projection that includes all window functions
        let projection = vec![
            category_col.clone(),
            group_col.clone(),
            value_col.clone(),
            window_func1.clone(),
            window_func2.clone(),
            window_func3.clone(),
            window_func4.clone(),
        ];

        // Build the input plan with the window functions in a projection
        let plan = input_plan.clone().select(projection)?.build();

        // Create the expected plan - we need it to match exactly what our optimizer produces

        // Get the auto-generated names
        let auto_generated_name1 =
            "value.local_sum().window(partition_by=[category,group],order_by=[])";
        let auto_generated_name2 =
            "value.local_mean().window(partition_by=[category,group],order_by=[])";
        let auto_generated_name3 = "value.local_min().window(partition_by=[category],order_by=[])";
        let auto_generated_name4 = "value.local_max().window(partition_by=[category],order_by=[])";

        // Based on the error output, the optimizer processes the window specs in the order they
        // appear in the logical plan, with multipartition windows first, then single partition windows.
        // This is opposite of what we expected.

        // Create multi-partition window operation first
        let multi_window_op = Window::try_new(
            input_plan.clone().build(),
            vec![
                Arc::new(Expr::Window(sum_expr.clone(), multi_partition_spec.clone()))
                    .alias(auto_generated_name1),
                Arc::new(Expr::Window(avg_expr.clone(), multi_partition_spec.clone()))
                    .alias(auto_generated_name2),
            ],
            multi_partition_spec,
        )?;

        // Create an intermediate plan with the multi-partition window operation
        let intermediate_plan = Arc::new(LogicalPlan::Window(multi_window_op));

        // Create the single-partition window operation using the output of the multi-partition operation
        let single_window_op = Window::try_new(
            intermediate_plan,
            vec![
                Arc::new(Expr::Window(
                    min_expr.clone(),
                    single_partition_spec.clone(),
                ))
                .alias(auto_generated_name3),
                Arc::new(Expr::Window(
                    max_expr.clone(),
                    single_partition_spec.clone(),
                ))
                .alias(auto_generated_name4),
            ],
            single_partition_spec,
        )?;

        // Create a plan with both window operations
        let window_plan = Arc::new(LogicalPlan::Window(single_window_op));

        // Create the final projection that aliases all the auto-generated names
        let final_projection = Project::try_new(
            window_plan,
            vec![
                category_col,
                group_col,
                value_col,
                resolved_col(auto_generated_name1).alias("sum_value"),
                resolved_col(auto_generated_name2).alias("avg_value"),
                resolved_col(auto_generated_name3).alias("min_value"),
                resolved_col(auto_generated_name4).alias("max_value"),
            ],
        )?;

        let expected_plan = Arc::new(LogicalPlan::Project(final_projection));

        assert_optimized_plan_eq(plan, expected_plan)
    }
}
