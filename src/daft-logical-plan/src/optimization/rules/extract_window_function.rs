use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_dsl::{Expr, ExprRef, WindowExpr, expr::window::WindowSpec, resolved_col};
use daft_schema::schema::Schema;
use indexmap::{IndexMap, IndexSet};

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

    #[allow(clippy::type_complexity)]
    fn extract_window_functions(
        projection: &[ExprRef],
        schema: &Schema,
    ) -> (Vec<(ExprRef, Arc<WindowSpec>)>, IndexMap<String, ExprRef>) {
        let mut result = Vec::new();
        // Track unique window functions by semantic ID
        let mut seen_window_exprs = IndexSet::new();
        let mut window_expr_mappings = IndexMap::new();

        for expr in projection {
            expr.apply(|e| {
                if let Expr::Over(_inner_expr, window_spec) = e.as_ref() {
                    let sem_id = e.semantic_id(schema);
                    let semantic_id_str = sem_id.id.to_string();

                    // If we've already seen this window expression, don't add it again
                    if !seen_window_exprs.contains(&sem_id) {
                        seen_window_exprs.insert(sem_id);
                        result.push((e.clone(), window_spec.clone()));
                    }

                    // Track the mapping from semantic ID to window expression
                    window_expr_mappings.insert(semantic_id_str, e.clone());

                    Ok(TreeNodeRecursion::Jump)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
            .unwrap();
        }

        (result, window_expr_mappings)
    }

    fn replace_window_functions(
        expr: &ExprRef,
        window_col_mappings: &[(ExprRef, String)],
        window_expr_mappings: &IndexMap<String, ExprRef>,
        schema: &Schema,
    ) -> DaftResult<ExprRef> {
        let transformed = expr.clone().transform(|e| {
            // Check if this is a window expression
            if let Expr::Over(_, _) = e.as_ref() {
                let sem_id = e.semantic_id(schema);
                let sem_id_str = sem_id.id.to_string();

                // If it's in our window mappings, find the corresponding output column
                if window_expr_mappings.contains_key(&sem_id_str) {
                    for (window_expr, col_name) in window_col_mappings {
                        if let Expr::Over(_, _) = window_expr.as_ref() {
                            let window_sem_id = window_expr.semantic_id(schema);
                            if window_sem_id == sem_id {
                                return Ok(Transformed::yes(resolved_col(col_name.clone())));
                            }
                        }
                    }
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
    /// 1. Find all window functions in a projection and deduplicate by semantic ID
    /// 2. Group them by their window specifications
    /// 3. Create a separate Window operation for each group
    /// 4. Connect these Window operations in a chain
    /// 5. Replace the original window expressions with column references
    /// 6. Create a final Project operation with the updated expressions
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| match node.as_ref() {
            LogicalPlan::Project(project) => {
                // Extract unique window functions and track mappings
                let (window_funcs, window_expr_mappings) =
                    Self::extract_window_functions(&project.projection, &project.input.schema());

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
                    window_funcs_grouped_by_spec.into_iter().try_fold(
                        (project.input.clone(), Vec::new()),
                        |(current_plan, mut window_col_mappings),
                         (window_spec, window_exprs_for_spec)|
                         -> DaftResult<_> {
                            // Split into separate steps for better error handling
                            let window_expr_pairs = window_exprs_for_spec
                                .iter()
                                .map(|e| {
                                    let semantic_id = e.semantic_id(&current_plan.schema());
                                    let window_expr = match e.as_ref() {
                                        Expr::Over(window_expr, _) => Ok(window_expr),
                                        _ => Err(DaftError::TypeError(format!(
                                            "Expected WindowExpr, got {:?}",
                                            e
                                        ))),
                                    }?;
                                    Ok((window_expr.clone(), semantic_id.id.to_string()))
                                })
                                .collect::<DaftResult<Vec<(WindowExpr, String)>>>()?;

                            // Unzip the pairs into separate vectors
                            let (window_functions, aliases): (Vec<WindowExpr>, Vec<String>) =
                                window_expr_pairs.into_iter().unzip();

                            let new_plan = Arc::new(LogicalPlan::Window(Window::try_new(
                                current_plan,
                                window_functions,
                                aliases,
                                window_spec,
                            )?));

                            let spec_mappings = window_exprs_for_spec
                                .iter()
                                .map(|e| {
                                    let semantic_id = e.semantic_id(&new_plan.schema());
                                    (e.clone(), semantic_id.id.to_string())
                                })
                                .collect::<Vec<(ExprRef, String)>>();

                            window_col_mappings.extend(spec_mappings);
                            Ok((new_plan, window_col_mappings))
                        },
                    )?;

                let new_projection = project
                    .projection
                    .iter()
                    .map(|expr| {
                        Self::replace_window_functions(
                            expr,
                            &window_col_mappings,
                            &window_expr_mappings,
                            &current_plan.schema(),
                        )
                    })
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
    use daft_dsl::{Expr, WindowExpr, expr::window::WindowSpec, resolved_col};
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
        let window_spec = Arc::new(window_spec);

        let min_expr: WindowExpr = value_col.clone().min().try_into().unwrap();

        let window_func =
            Arc::new(Expr::Over(min_expr.clone(), window_spec.clone())).alias("min_value");

        let projection = vec![category_col.clone(), value_col.clone(), window_func.clone()];

        let plan = input_plan.clone().select(projection)?.build();

        let window_expr = Arc::new(Expr::Over(min_expr.clone(), window_spec.clone()));
        let auto_generated_name = window_expr.semantic_id(&input_plan.schema()).id.to_string();

        let window_op = Window::try_new(
            input_plan.clone().build(),
            vec![min_expr.clone()],
            vec![auto_generated_name.clone()],
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
        let window_spec = Arc::new(window_spec);

        let sum_expr: WindowExpr = value_col.clone().sum().try_into().unwrap();

        let window_func =
            Arc::new(Expr::Over(sum_expr.clone(), window_spec.clone())).alias("sum_value");

        let projection = vec![
            category_col.clone(),
            group_col.clone(),
            value_col.clone(),
            window_func.clone(),
        ];

        let plan = input_plan.clone().select(projection)?.build();

        let window_expr = Arc::new(Expr::Over(sum_expr.clone(), window_spec.clone()));
        let auto_generated_name = window_expr.semantic_id(&input_plan.schema()).id.to_string();

        let window_op = Window::try_new(
            input_plan.clone().build(),
            vec![sum_expr.clone()],
            vec![auto_generated_name.clone()],
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
        let window_spec1 = Arc::new(window_spec1);

        let mut window_spec2 = WindowSpec::default();
        window_spec2.partition_by = vec![group_col.clone()];
        let window_spec2 = Arc::new(window_spec2);

        let min_expr: WindowExpr = value_col.clone().min().try_into().unwrap();
        let sum_expr: WindowExpr = value_col.clone().sum().try_into().unwrap();

        let window_func1 =
            Arc::new(Expr::Over(min_expr.clone(), window_spec1.clone())).alias("min_by_category");
        let window_func2 =
            Arc::new(Expr::Over(sum_expr.clone(), window_spec2.clone())).alias("sum_by_group");

        let projection = vec![
            category_col.clone(),
            group_col.clone(),
            value_col.clone(),
            window_func1.clone(),
            window_func2.clone(),
        ];

        let plan = input_plan.clone().select(projection)?.build();

        let window_expr1 = Arc::new(Expr::Over(min_expr.clone(), window_spec1.clone()));
        let window_expr2 = Arc::new(Expr::Over(sum_expr.clone(), window_spec2.clone()));

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
            vec![min_expr.clone()],
            vec![auto_generated_name1.clone()],
            window_spec1,
        )?;

        let intermediate_plan = Arc::new(LogicalPlan::Window(window_op1));

        let window_op2 = Window::try_new(
            intermediate_plan,
            vec![sum_expr.clone()],
            vec![auto_generated_name2.clone()],
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
        let multi_partition_spec = Arc::new(multi_partition_spec);

        let mut single_partition_spec = WindowSpec::default();
        single_partition_spec.partition_by = vec![category_col.clone()];
        let single_partition_spec = Arc::new(single_partition_spec);

        let sum_expr: WindowExpr = value_col.clone().sum().try_into().unwrap();
        let avg_expr: WindowExpr = value_col.clone().mean().try_into().unwrap();
        let min_expr: WindowExpr = value_col.clone().min().try_into().unwrap();
        let max_expr: WindowExpr = value_col.clone().max().try_into().unwrap();

        let window_func1 =
            Arc::new(Expr::Over(sum_expr.clone(), multi_partition_spec.clone())).alias("sum_value");
        let window_func2 =
            Arc::new(Expr::Over(avg_expr.clone(), multi_partition_spec.clone())).alias("avg_value");

        let window_func3 = Arc::new(Expr::Over(min_expr.clone(), single_partition_spec.clone()))
            .alias("min_value");
        let window_func4 = Arc::new(Expr::Over(max_expr.clone(), single_partition_spec.clone()))
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

        let window_expr1 = Arc::new(Expr::Over(sum_expr.clone(), multi_partition_spec.clone()));
        let window_expr2 = Arc::new(Expr::Over(avg_expr.clone(), multi_partition_spec.clone()));
        let window_expr3 = Arc::new(Expr::Over(min_expr.clone(), single_partition_spec.clone()));
        let window_expr4 = Arc::new(Expr::Over(max_expr.clone(), single_partition_spec.clone()));

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
            vec![sum_expr.clone(), avg_expr.clone()],
            vec![auto_generated_name1.clone(), auto_generated_name2.clone()],
            multi_partition_spec,
        )?;

        let intermediate_plan = Arc::new(LogicalPlan::Window(multi_window_op));

        let single_window_op = Window::try_new(
            intermediate_plan,
            vec![min_expr.clone(), max_expr.clone()],
            vec![auto_generated_name3.clone(), auto_generated_name4.clone()],
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

    #[test]
    fn test_three_window_specs() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("letter", DataType::Utf8),
            Field::new("num", DataType::Utf8),
            Field::new("value", DataType::Int64),
        ]);

        let input_plan = dummy_scan_node(scan_op.clone());

        let letter_col = resolved_col("letter");
        let num_col = resolved_col("num");
        let value_col = resolved_col("value");

        let mut letter_window_spec = WindowSpec::default();
        letter_window_spec.partition_by = vec![letter_col.clone()];
        let letter_window_spec = Arc::new(letter_window_spec);

        let mut num_window_spec = WindowSpec::default();
        num_window_spec.partition_by = vec![num_col.clone()];
        let num_window_spec = Arc::new(num_window_spec);

        let mut combined_window_spec = WindowSpec::default();
        combined_window_spec.partition_by = vec![letter_col.clone(), num_col.clone()];
        let combined_window_spec = Arc::new(combined_window_spec);

        let letter_sum_expr: WindowExpr = value_col.clone().sum().try_into().unwrap();
        let num_sum_expr: WindowExpr = value_col.clone().sum().try_into().unwrap();
        let combined_sum_expr: WindowExpr = value_col.clone().sum().try_into().unwrap();

        let letter_sum = Arc::new(Expr::Over(
            letter_sum_expr.clone(),
            letter_window_spec.clone(),
        ))
        .alias("letter_sum");

        let num_sum =
            Arc::new(Expr::Over(num_sum_expr.clone(), num_window_spec.clone())).alias("num_sum");

        let combined_sum = Arc::new(Expr::Over(
            combined_sum_expr.clone(),
            combined_window_spec.clone(),
        ))
        .alias("combined_sum");

        let projection = vec![
            letter_col.clone(),
            num_col.clone(),
            value_col.clone(),
            letter_sum.clone(),
            num_sum.clone(),
            combined_sum.clone(),
        ];

        let plan = input_plan.clone().select(projection)?.build();

        let window_expr1 = Arc::new(Expr::Over(
            letter_sum_expr.clone(),
            letter_window_spec.clone(),
        ));
        let window_expr2 = Arc::new(Expr::Over(num_sum_expr.clone(), num_window_spec.clone()));
        let window_expr3 = Arc::new(Expr::Over(
            combined_sum_expr.clone(),
            combined_window_spec.clone(),
        ));

        let letter_sum_id = window_expr1
            .semantic_id(&input_plan.schema())
            .id
            .to_string();
        let num_sum_id = window_expr2
            .semantic_id(&input_plan.schema())
            .id
            .to_string();
        let combined_sum_id = window_expr3
            .semantic_id(&input_plan.schema())
            .id
            .to_string();

        let letter_window_op = Window::try_new(
            input_plan.clone().build(),
            vec![letter_sum_expr],
            vec![letter_sum_id.clone()],
            letter_window_spec,
        )?;

        let intermediate_plan1 = Arc::new(LogicalPlan::Window(letter_window_op));

        let num_window_op = Window::try_new(
            intermediate_plan1,
            vec![num_sum_expr],
            vec![num_sum_id.clone()],
            num_window_spec,
        )?;

        let intermediate_plan2 = Arc::new(LogicalPlan::Window(num_window_op));

        let combined_window_op = Window::try_new(
            intermediate_plan2,
            vec![combined_sum_expr],
            vec![combined_sum_id.clone()],
            combined_window_spec,
        )?;

        let window_plan = Arc::new(LogicalPlan::Window(combined_window_op));

        let final_projection = Project::try_new(
            window_plan,
            vec![
                letter_col,
                num_col,
                value_col,
                resolved_col(letter_sum_id).alias("letter_sum"),
                resolved_col(num_sum_id).alias("num_sum"),
                resolved_col(combined_sum_id).alias("combined_sum"),
            ],
        )?;

        let expected_plan = Arc::new(LogicalPlan::Project(final_projection));

        assert_optimized_plan_eq(plan, expected_plan)
    }

    #[test]
    fn test_duplicate_window_functions() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("category", DataType::Utf8),
            Field::new("value", DataType::Int64),
        ]);

        let input_plan = dummy_scan_node(scan_op.clone());

        let category_col = resolved_col("category");
        let value_col = resolved_col("value");

        let mut window_spec = WindowSpec::default();
        window_spec.partition_by = vec![category_col.clone()];
        let window_spec = Arc::new(window_spec);

        let min_expr: WindowExpr = value_col.clone().min().try_into().unwrap();

        let window_func1 =
            Arc::new(Expr::Over(min_expr.clone(), window_spec.clone())).alias("min_value1");
        let window_func2 =
            Arc::new(Expr::Over(min_expr.clone(), window_spec.clone())).alias("min_value2");

        let projection = vec![
            category_col.clone(),
            value_col.clone(),
            window_func1.clone(),
            window_func2.clone(),
        ];

        let plan = input_plan.clone().select(projection)?.build();

        let window_expr = Arc::new(Expr::Over(min_expr.clone(), window_spec.clone()));
        let auto_generated_name = window_expr.semantic_id(&input_plan.schema()).id.to_string();

        let window_op = Window::try_new(
            input_plan.clone().build(),
            vec![min_expr.clone()],
            vec![auto_generated_name.clone()],
            window_spec,
        )?;

        let window_plan = Arc::new(LogicalPlan::Window(window_op));
        let final_projection = Project::try_new(
            window_plan,
            vec![
                category_col,
                value_col,
                resolved_col(auto_generated_name.clone()).alias("min_value1"),
                resolved_col(auto_generated_name).alias("min_value2"),
            ],
        )?;

        let expected_plan = Arc::new(LogicalPlan::Project(final_projection));

        assert_optimized_plan_eq(plan, expected_plan)
    }
}
