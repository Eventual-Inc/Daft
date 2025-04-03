use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_dsl::{expr::window::WindowSpec, resolved_col, Expr, ExprRef};
use indexmap::IndexMap;

use crate::{
    logical_plan::{LogicalPlan, Project},
    ops::Window,
    optimization::rules::OptimizerRule,
};

#[derive(Debug)]
pub struct ExtractWindowFunction;

impl Default for ExtractWindowFunction {
    fn default() -> Self {
        Self
    }
}

impl ExtractWindowFunction {
    pub fn new() -> Self {
        Self
    }

    fn extract_window_functions(projection: &[ExprRef]) -> Vec<(ExprRef, WindowSpec)> {
        let mut result = Vec::new();

        for expr in projection {
            expr.apply(|e| {
                if let Expr::Window(_inner_expr, window_spec) = e.as_ref() {
                    result.push((e.clone(), window_spec.clone()));
                    Ok(TreeNodeRecursion::Jump)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
            .unwrap();
        }

        result
    }

    fn replace_window_functions(
        expr: &ExprRef,
        window_col_mappings: &[(ExprRef, String)],
    ) -> DaftResult<ExprRef> {
        let transformed = expr.clone().transform(|e| {
            for (window_expr, col_name) in window_col_mappings {
                if Arc::ptr_eq(&e, window_expr) {
                    return Ok(Transformed::yes(resolved_col(col_name.clone())));
                }
            }
            Ok(Transformed::no(e))
        })?;

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
        plan.transform_down(|node| match node.as_ref() {
            LogicalPlan::Project(project) => {
                let window_funcs = Self::extract_window_functions(&project.projection);

                if window_funcs.is_empty() {
                    return Ok(Transformed::no(node));
                }

                // Group window functions by spec using IndexMap for deterministic ordering
                let mut window_funcs_grouped_by_spec = IndexMap::new();
                for (expr, spec) in window_funcs {
                    window_funcs_grouped_by_spec
                        .entry(spec)
                        .or_insert_with(Vec::new)
                        .push(expr);
                }

                let (current_plan, window_col_mappings) =
                    window_funcs_grouped_by_spec.into_iter().fold(
                        (project.input.clone(), Vec::new()),
                        |(current_plan, mut window_col_mappings),
                         (window_spec, window_exprs_for_spec)| {
                            // Semantic IDs to alias expressions in the Window operation
                            let window_function_exprs_aliased = window_exprs_for_spec
                                .iter()
                                .map(|e| {
                                    let semantic_id = e.semantic_id(&current_plan.schema());
                                    e.alias(semantic_id.id)
                                })
                                .collect::<Vec<ExprRef>>();

                            let new_plan = Arc::new(LogicalPlan::Window(
                                Window::try_new(
                                    current_plan,
                                    window_function_exprs_aliased,
                                    window_spec,
                                )
                                .unwrap(),
                            ));

                            // Semantic IDs to map original window expressions to column names in the final projection
                            let spec_mappings = window_exprs_for_spec
                                .iter()
                                .map(|e| {
                                    let semantic_id = e.semantic_id(&new_plan.schema());
                                    (e.clone(), semantic_id.id.to_string())
                                })
                                .collect::<Vec<(ExprRef, String)>>();

                            window_col_mappings.extend(spec_mappings);
                            (new_plan, window_col_mappings)
                        },
                    );

                let new_projection = project
                    .projection
                    .iter()
                    .map(|expr| Self::replace_window_functions(expr, &window_col_mappings))
                    .collect::<DaftResult<Vec<ExprRef>>>()?;

                let final_plan = Arc::new(LogicalPlan::Project(Project::try_new(
                    current_plan,
                    new_projection,
                )?));

                Ok(Transformed::yes(final_plan))
            }
            _ => Ok(Transformed::no(node)),
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

    #[test]
    fn test_single_partition_window_function() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("category", DataType::Utf8),
            Field::new("value", DataType::Int64),
        ]);

        let input_plan = dummy_scan_node(scan_op.clone());

        let category_col = resolved_col("category");
        let value_col = resolved_col("value");

        let mut window_spec = WindowSpec::default();
        window_spec.partition_by = vec![category_col.clone()];

        let min_expr = value_col.clone().min();

        let window_func =
            Arc::new(Expr::Window(min_expr.clone(), window_spec.clone())).alias("min_value");

        let projection = vec![category_col.clone(), value_col.clone(), window_func.clone()];

        let plan = input_plan.clone().select(projection)?.build();

        let window_expr = Arc::new(Expr::Window(min_expr.clone(), window_spec.clone()));
        let auto_generated_name = window_expr.semantic_id(&input_plan.schema()).id.to_string();

        let window_op = Window::try_new(
            input_plan.clone().build(),
            vec![
                Arc::new(Expr::Window(min_expr.clone(), window_spec.clone()))
                    .alias(auto_generated_name.clone()),
            ],
            window_spec,
        )?;

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

    #[test]
    fn test_multiple_partition_window_function() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ]);

        let input_plan = dummy_scan_node(scan_op.clone());

        let category_col = resolved_col("category");
        let group_col = resolved_col("group");
        let value_col = resolved_col("value");

        let mut window_spec = WindowSpec::default();
        window_spec.partition_by = vec![category_col.clone(), group_col.clone()];

        let sum_expr = value_col.clone().sum();

        let window_func =
            Arc::new(Expr::Window(sum_expr.clone(), window_spec.clone())).alias("sum_value");

        let projection = vec![
            category_col.clone(),
            group_col.clone(),
            value_col.clone(),
            window_func.clone(),
        ];

        let plan = input_plan.clone().select(projection)?.build();

        let auto_generated_name =
            "value.local_sum().window(partition_by=[category,group],order_by=[])";

        let window_op = Window::try_new(
            input_plan.clone().build(),
            vec![
                Arc::new(Expr::Window(sum_expr.clone(), window_spec.clone()))
                    .alias(auto_generated_name),
            ],
            window_spec,
        )?;

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

    #[test]
    fn test_multiple_window_specs() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ]);

        let input_plan = dummy_scan_node(scan_op.clone());

        let category_col = resolved_col("category");
        let group_col = resolved_col("group");
        let value_col = resolved_col("value");

        let mut window_spec1 = WindowSpec::default();
        window_spec1.partition_by = vec![category_col.clone()];

        let mut window_spec2 = WindowSpec::default();
        window_spec2.partition_by = vec![group_col.clone()];

        let min_expr = value_col.clone().min();
        let sum_expr = value_col.clone().sum();

        let window_func1 =
            Arc::new(Expr::Window(min_expr.clone(), window_spec1.clone())).alias("min_by_category");
        let window_func2 =
            Arc::new(Expr::Window(sum_expr.clone(), window_spec2.clone())).alias("sum_by_group");

        let projection = vec![
            category_col.clone(),
            group_col.clone(),
            value_col.clone(),
            window_func1.clone(),
            window_func2.clone(),
        ];

        let plan = input_plan.clone().select(projection)?.build();

        let window_expr1 = Arc::new(Expr::Window(min_expr.clone(), window_spec1.clone()));
        let window_expr2 = Arc::new(Expr::Window(sum_expr.clone(), window_spec2.clone()));

        let auto_generated_name1 = window_expr1
            .semantic_id(&input_plan.schema())
            .id
            .to_string();
        let auto_generated_name2 = window_expr2
            .semantic_id(&input_plan.schema())
            .id
            .to_string();

        let window_op1 = Window::try_new(
            input_plan.clone().build(),
            vec![
                Arc::new(Expr::Window(min_expr.clone(), window_spec1.clone()))
                    .alias(auto_generated_name1.clone()),
            ],
            window_spec1,
        )?;

        let intermediate_plan = Arc::new(LogicalPlan::Window(window_op1));

        let window_op2 = Window::try_new(
            intermediate_plan,
            vec![
                Arc::new(Expr::Window(sum_expr.clone(), window_spec2.clone()))
                    .alias(auto_generated_name2.clone()),
            ],
            window_spec2,
        )?;

        let window_plan = Arc::new(LogicalPlan::Window(window_op2));

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

    #[test]
    fn test_multiple_window_functions() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ]);

        let input_plan = dummy_scan_node(scan_op.clone());

        let category_col = resolved_col("category");
        let group_col = resolved_col("group");
        let value_col = resolved_col("value");

        let mut multi_partition_spec = WindowSpec::default();
        multi_partition_spec.partition_by = vec![category_col.clone(), group_col.clone()];

        let mut single_partition_spec = WindowSpec::default();
        single_partition_spec.partition_by = vec![category_col.clone()];

        let sum_expr = value_col.clone().sum();
        let avg_expr = value_col.clone().mean();
        let min_expr = value_col.clone().min();
        let max_expr = value_col.clone().max();

        let window_func1 = Arc::new(Expr::Window(sum_expr.clone(), multi_partition_spec.clone()))
            .alias("sum_value");
        let window_func2 = Arc::new(Expr::Window(avg_expr.clone(), multi_partition_spec.clone()))
            .alias("avg_value");

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

        let projection = vec![
            category_col.clone(),
            group_col.clone(),
            value_col.clone(),
            window_func1.clone(),
            window_func2.clone(),
            window_func3.clone(),
            window_func4.clone(),
        ];

        let plan = input_plan.clone().select(projection)?.build();

        let window_expr1 = Arc::new(Expr::Window(sum_expr.clone(), multi_partition_spec.clone()));
        let window_expr2 = Arc::new(Expr::Window(avg_expr.clone(), multi_partition_spec.clone()));
        let window_expr3 = Arc::new(Expr::Window(
            min_expr.clone(),
            single_partition_spec.clone(),
        ));
        let window_expr4 = Arc::new(Expr::Window(
            max_expr.clone(),
            single_partition_spec.clone(),
        ));

        let auto_generated_name1 = window_expr1
            .semantic_id(&input_plan.schema())
            .id
            .to_string();
        let auto_generated_name2 = window_expr2
            .semantic_id(&input_plan.schema())
            .id
            .to_string();
        let auto_generated_name3 = window_expr3
            .semantic_id(&input_plan.schema())
            .id
            .to_string();
        let auto_generated_name4 = window_expr4
            .semantic_id(&input_plan.schema())
            .id
            .to_string();

        let multi_window_op = Window::try_new(
            input_plan.clone().build(),
            vec![
                Arc::new(Expr::Window(sum_expr.clone(), multi_partition_spec.clone()))
                    .alias(auto_generated_name1.clone()),
                Arc::new(Expr::Window(avg_expr.clone(), multi_partition_spec.clone()))
                    .alias(auto_generated_name2.clone()),
            ],
            multi_partition_spec,
        )?;

        let intermediate_plan = Arc::new(LogicalPlan::Window(multi_window_op));

        let single_window_op = Window::try_new(
            intermediate_plan,
            vec![
                Arc::new(Expr::Window(
                    min_expr.clone(),
                    single_partition_spec.clone(),
                ))
                .alias(auto_generated_name3.clone()),
                Arc::new(Expr::Window(
                    max_expr.clone(),
                    single_partition_spec.clone(),
                ))
                .alias(auto_generated_name4.clone()),
            ],
            single_partition_spec,
        )?;

        let window_plan = Arc::new(LogicalPlan::Window(single_window_op));

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
