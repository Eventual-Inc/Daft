use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{AggExpr, Column, Expr, ExprRef, ResolvedColumn};

use crate::{
    logical_plan::{LogicalPlan, Project},
    ops::{Aggregate, Filter, Join, MonotonicallyIncreasingId},
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
    /// or contains a nested monotonically_increasing_id() call
    fn is_monotonic_id_expr(expr: &ExprRef) -> bool {
        match expr.as_ref() {
            // Direct function call
            Expr::ScalarFunction(func) => func.name() == "monotonically_increasing_id",

            // Expression wrapping another expression
            Expr::Alias(inner, _) => Self::is_monotonic_id_expr(inner),
            Expr::Cast(inner, _) => Self::is_monotonic_id_expr(inner),
            Expr::Not(inner) => Self::is_monotonic_id_expr(inner),
            Expr::IsNull(inner) => Self::is_monotonic_id_expr(inner),
            Expr::NotNull(inner) => Self::is_monotonic_id_expr(inner),
            Expr::FillNull(inner, fill_value) => {
                Self::is_monotonic_id_expr(inner) || Self::is_monotonic_id_expr(fill_value)
            }

            // Binary operations
            Expr::BinaryOp { left, right, .. } => {
                Self::is_monotonic_id_expr(left) || Self::is_monotonic_id_expr(right)
            }

            // Multi-argument operations
            Expr::IsIn(expr, list) => {
                Self::is_monotonic_id_expr(expr) || list.iter().any(Self::is_monotonic_id_expr)
            }
            Expr::Between(expr, low, high) => {
                Self::is_monotonic_id_expr(expr)
                    || Self::is_monotonic_id_expr(low)
                    || Self::is_monotonic_id_expr(high)
            }
            Expr::List(exprs) => exprs.iter().any(Self::is_monotonic_id_expr),
            Expr::IfElse {
                predicate,
                if_true,
                if_false,
            } => {
                Self::is_monotonic_id_expr(predicate)
                    || Self::is_monotonic_id_expr(if_true)
                    || Self::is_monotonic_id_expr(if_false)
            }
            Expr::Function { inputs, .. } => inputs.iter().any(Self::is_monotonic_id_expr),

            // All other expressions don't contain monotonically_increasing_id
            _ => false,
        }
    }

    /// Helper function to find all monotonically_increasing_id() expressions in a Project operation
    /// Returns a vector of (column_name, original_expr) pairs for each monotonic ID found
    fn find_monotonic_ids(project: &Project) -> Vec<(String, ExprRef)> {
        project
            .projection
            .iter()
            .filter(|expr| Self::is_monotonic_id_expr(expr))
            .map(|expr| {
                // Use expr.name() which correctly handles nested aliases
                let column_name = expr.name().to_string();
                // If name is empty or from a non-aliased monotonic_id function, use "id" as default
                let column_name =
                    if column_name.is_empty() || column_name == "monotonically_increasing_id" {
                        "id".to_string()
                    } else {
                        column_name
                    };
                (column_name, expr.clone())
            })
            .collect()
    }

    /// Find monotonic_id calls in filter predicate
    fn find_monotonic_ids_in_filter(filter: &Filter) -> Vec<(String, ExprRef)> {
        let mut result = HashSet::new();

        // Check the predicate for monotonic ID expressions by recursively collecting all calls
        let predicate_expr = filter.predicate.as_ref().clone();
        Self::collect_monotonic_id_calls(&[predicate_expr], &mut result)
    }

    /// Helper function to find all monotonically_increasing_id() expressions in join conditions
    /// Returns a vector of (column_name, original_expr) pairs for each monotonic ID found
    fn find_monotonic_ids_in_join(join: &Join) -> Vec<(String, ExprRef)> {
        let mut result = Vec::new();

        // For join_on conditions that use binary operations (like a == b), we need to
        // check if either side contains monotonically_increasing_id
        let mut process_expr = |expr: &ExprRef| {
            if Self::is_monotonic_id_expr(expr) {
                let column_name = match expr.as_ref() {
                    Expr::Alias(_, name) => name.to_string(),
                    _ => "id".to_string(), // Default name if not aliased
                };
                if !result.iter().any(|(n, _)| n == &column_name) {
                    result.push((column_name, expr.clone()));
                }
            }
        };

        // Check left_on expressions
        for expr in &join.left_on {
            process_expr(expr);
        }

        // Check right_on expressions
        for expr in &join.right_on {
            process_expr(expr);
        }

        result
    }

    /// Helper function to create a chain of MonotonicallyIncreasingId operations
    fn create_monotonic_chain(
        input: Arc<LogicalPlan>,
        monotonic_ids: Vec<(String, ExprRef)>,
    ) -> DaftResult<Arc<LogicalPlan>> {
        let mut current_plan = input;

        // If we have multiple monotonically_increasing_id() calls in a single operation,
        // we only need to create one MonotonicallyIncreasingId operation since they will
        // all have the same values
        if !monotonic_ids.is_empty() {
            let first_column = monotonic_ids[0].0.clone();
            current_plan = Arc::new(LogicalPlan::MonotonicallyIncreasingId(
                MonotonicallyIncreasingId::try_new(current_plan, Some(&first_column))?,
            ));
        }

        Ok(current_plan)
    }

    /// Helper function to create a new projection list that preserves the original order
    /// and replaces monotonically_increasing_id() calls with column references
    fn create_new_projection(
        original_projection: &[ExprRef],
        monotonic_ids: &[(String, ExprRef)],
    ) -> Vec<ExprRef> {
        // If we have monotonic IDs, use the first one as the main ID column
        let first_column = monotonic_ids.first().map(|(name, _)| name.clone());

        original_projection
            .iter()
            .map(|expr| {
                if Self::is_monotonic_id_expr(expr) {
                    // Find the corresponding column name for this monotonic ID
                    let column_name = match expr.as_ref() {
                        Expr::Alias(_, name) => name.clone(),
                        _ => {
                            if let Some(first) = &first_column {
                                Arc::from(first.as_str())
                            } else {
                                Arc::from("id")
                            }
                        }
                    };

                    // If this is not the first ID column but is an alias, create an alias of the first column
                    if let Some(first) = &first_column {
                        if column_name.to_string() != *first {
                            // Create an alias of the first column with this name
                            let first_col = Expr::Column(Column::Resolved(ResolvedColumn::Basic(
                                Arc::from(first.as_str()),
                            )))
                            .into();
                            return Expr::Alias(first_col, column_name).into();
                        }
                    }

                    // Replace with a column reference
                    Expr::Column(Column::Resolved(ResolvedColumn::Basic(column_name))).into()
                } else {
                    expr.clone()
                }
            })
            .collect()
    }

    /// Helper function to process the filter predicate and add proper column names
    /// for monotonically_increasing_id() calls
    fn create_new_filter_predicate(
        original_predicate: &ExprRef,
        monotonic_ids: &[(String, ExprRef)],
    ) -> ExprRef {
        // If the entire predicate is a monotonic_id function, replace it directly with a column reference
        if Self::is_monotonic_id_expr(original_predicate)
            && !matches!(original_predicate.as_ref(), Expr::BinaryOp { .. })
        {
            // If we have monotonic IDs, use the first one as the main ID column
            let first_column = monotonic_ids.first().map(|(name, _)| name.clone());

            if let Some(first) = first_column {
                // Replace with a column reference
                Expr::Column(Column::Resolved(ResolvedColumn::Basic(Arc::from(first)))).into()
            } else {
                // This shouldn't happen as we've already checked that monotonic_ids is not empty
                original_predicate.clone()
            }
        } else {
            // For more complex expressions containing monotonic_id calls, we need to recursively replace them
            match original_predicate.as_ref() {
                Expr::Alias(inner, name) => {
                    let new_inner = Self::create_new_filter_predicate(inner, monotonic_ids);
                    Expr::Alias(new_inner, name.clone()).into()
                }
                Expr::Cast(inner, data_type) => {
                    let new_inner = Self::create_new_filter_predicate(inner, monotonic_ids);
                    Expr::Cast(new_inner, data_type.clone()).into()
                }
                Expr::Not(inner) => {
                    let new_inner = Self::create_new_filter_predicate(inner, monotonic_ids);
                    Expr::Not(new_inner).into()
                }
                Expr::IsNull(inner) => {
                    let new_inner = Self::create_new_filter_predicate(inner, monotonic_ids);
                    Expr::IsNull(new_inner).into()
                }
                Expr::NotNull(inner) => {
                    let new_inner = Self::create_new_filter_predicate(inner, monotonic_ids);
                    Expr::NotNull(new_inner).into()
                }
                Expr::FillNull(inner, fill_value) => {
                    let new_inner = Self::create_new_filter_predicate(inner, monotonic_ids);
                    let new_fill = Self::create_new_filter_predicate(fill_value, monotonic_ids);
                    Expr::FillNull(new_inner, new_fill).into()
                }
                Expr::BinaryOp { left, right, op } => {
                    let new_left = Self::create_new_filter_predicate(left, monotonic_ids);
                    let new_right = Self::create_new_filter_predicate(right, monotonic_ids);

                    // If one side of the binary op is a monotonic_id, directly replace it with the column reference
                    let final_left = if Self::is_monotonic_id_expr(left) {
                        let first_column = monotonic_ids.first().map(|(name, _)| name.clone());
                        if let Some(first) = first_column {
                            Expr::Column(Column::Resolved(ResolvedColumn::Basic(Arc::from(
                                first.as_str(),
                            ))))
                            .into()
                        } else {
                            new_left
                        }
                    } else {
                        new_left
                    };

                    Expr::BinaryOp {
                        left: final_left,
                        right: new_right,
                        op: *op,
                    }
                    .into()
                }
                Expr::IsIn(expr, list) => {
                    let new_expr = Self::create_new_filter_predicate(expr, monotonic_ids);
                    let new_list = list
                        .iter()
                        .map(|e| Self::create_new_filter_predicate(e, monotonic_ids))
                        .collect();
                    Expr::IsIn(new_expr, new_list).into()
                }
                Expr::Between(expr, low, high) => {
                    let new_expr = Self::create_new_filter_predicate(expr, monotonic_ids);
                    let new_low = Self::create_new_filter_predicate(low, monotonic_ids);
                    let new_high = Self::create_new_filter_predicate(high, monotonic_ids);
                    Expr::Between(new_expr, new_low, new_high).into()
                }
                Expr::List(exprs) => {
                    let new_exprs = exprs
                        .iter()
                        .map(|e| Self::create_new_filter_predicate(e, monotonic_ids))
                        .collect();
                    Expr::List(new_exprs).into()
                }
                Expr::IfElse {
                    predicate,
                    if_true,
                    if_false,
                } => {
                    let new_predicate = Self::create_new_filter_predicate(predicate, monotonic_ids);
                    let new_if_true = Self::create_new_filter_predicate(if_true, monotonic_ids);
                    let new_if_false = Self::create_new_filter_predicate(if_false, monotonic_ids);
                    Expr::IfElse {
                        predicate: new_predicate,
                        if_true: new_if_true,
                        if_false: new_if_false,
                    }
                    .into()
                }
                Expr::Function { inputs, func } => {
                    let new_inputs = inputs
                        .iter()
                        .map(|expr| Self::create_new_filter_predicate(expr, monotonic_ids))
                        .collect();
                    Expr::Function {
                        func: func.clone(),
                        inputs: new_inputs,
                    }
                    .into()
                }
                // For expressions that don't contain monotonic_id, just return as is
                _ => original_predicate.clone(),
            }
        }
    }

    /// Helper function to create new join conditions that replace monotonic_id calls with columns
    fn create_new_join_conditions(
        original_exprs: &[ExprRef],
        monotonic_ids: &[(String, ExprRef)],
    ) -> Vec<ExprRef> {
        // If we have monotonic IDs, use the first one as the main ID column
        let first_column = monotonic_ids.first().map(|(name, _)| name.clone());

        original_exprs
            .iter()
            .map(|expr| {
                if Self::is_monotonic_id_expr(expr) {
                    // Use the first ID column
                    if let Some(first) = &first_column {
                        // Replace with a column reference
                        Expr::Column(Column::Resolved(ResolvedColumn::Basic(Arc::from(
                            first.as_str(),
                        ))))
                        .into()
                    } else {
                        // If there are no monotonic IDs found (shouldn't happen), keep original
                        expr.clone()
                    }
                } else {
                    // For more complex expressions containing monotonic_id, use the same logic as for filters
                    Self::create_new_filter_predicate(expr, monotonic_ids)
                }
            })
            .collect()
    }

    /// Helper function to find all monotonically_increasing_id() expressions in Aggregate operations
    /// Returns a vector of (column_name, original_expr) pairs for each monotonic ID found
    fn find_monotonic_ids_in_aggregate(aggregate: &Aggregate) -> Vec<(String, ExprRef)> {
        let mut result = HashSet::new();

        // Check aggregation expressions
        for expr in &aggregate.aggregations {
            let expr_clone = expr.as_ref().clone();
            Self::collect_monotonic_id_calls(&[expr_clone], &mut result);
        }

        // Check groupby expressions
        for expr in &aggregate.groupby {
            let expr_clone = expr.as_ref().clone();
            Self::collect_monotonic_id_calls(&[expr_clone], &mut result);
        }

        // Convert to our result format - for each monotonic ID, get a reasonable column name
        // If it's an alias, use that alias, otherwise default to "id"
        let mut named_results = Vec::new();
        for expr in result {
            let name = if let Expr::Alias(_, name) = &expr {
                name.to_string()
            } else {
                "id".to_string()
            };

            named_results.push((name, ExprRef::from(expr)));
        }

        named_results
    }

    /// Helper function to recursively collect all monotonically_increasing_id() expressions from an expression tree
    fn collect_monotonic_id_calls(
        exprs: &[Expr],
        result: &mut HashSet<Expr>,
    ) -> Vec<(String, ExprRef)> {
        // For recursive collection

        for expr in exprs {
            // Check if the expression is a direct monotonically_increasing_id call
            if let Expr::ScalarFunction(func) = expr {
                if func.name() == "monotonically_increasing_id" && func.inputs.is_empty() {
                    result.insert(expr.clone());

                    continue;
                }
            }

            // Handle different expression types
            match expr {
                Expr::Alias(aliased_expr, _name) => {
                    // Recursively check the aliased expression
                    Self::collect_monotonic_id_calls(&[aliased_expr.as_ref().clone()], result);
                }
                Expr::Cast(cast_expr, _) => {
                    Self::collect_monotonic_id_calls(&[cast_expr.as_ref().clone()], result);
                }
                Expr::Not(inner_expr) => {
                    Self::collect_monotonic_id_calls(&[inner_expr.as_ref().clone()], result);
                }
                Expr::IsNull(inner_expr) => {
                    Self::collect_monotonic_id_calls(&[inner_expr.as_ref().clone()], result);
                }
                Expr::NotNull(inner_expr) => {
                    Self::collect_monotonic_id_calls(&[inner_expr.as_ref().clone()], result);
                }
                Expr::FillNull(inner_expr, fill_value) => {
                    Self::collect_monotonic_id_calls(&[inner_expr.as_ref().clone()], result);
                    Self::collect_monotonic_id_calls(&[fill_value.as_ref().clone()], result);
                }
                Expr::BinaryOp { left, right, .. } => {
                    Self::collect_monotonic_id_calls(
                        &[left.as_ref().clone(), right.as_ref().clone()],
                        result,
                    );
                }
                Expr::IfElse {
                    if_true,
                    if_false,
                    predicate,
                } => {
                    Self::collect_monotonic_id_calls(
                        &[
                            if_true.as_ref().clone(),
                            if_false.as_ref().clone(),
                            predicate.as_ref().clone(),
                        ],
                        result,
                    );
                }
                Expr::IsIn(expr, list) => {
                    Self::collect_monotonic_id_calls(&[expr.as_ref().clone()], result);

                    // Convert list to a vector of Expr
                    let list_exprs: Vec<Expr> = list.iter().map(|e| e.as_ref().clone()).collect();
                    Self::collect_monotonic_id_calls(&list_exprs, result);
                }
                Expr::Between(expr, low, high) => {
                    Self::collect_monotonic_id_calls(&[expr.as_ref().clone()], result);
                    Self::collect_monotonic_id_calls(
                        &[low.as_ref().clone(), high.as_ref().clone()],
                        result,
                    );
                }
                Expr::List(exprs) => {
                    // Convert exprs to a vector of Expr
                    let list_exprs: Vec<Expr> = exprs.iter().map(|e| e.as_ref().clone()).collect();
                    Self::collect_monotonic_id_calls(&list_exprs, result);
                }
                Expr::Function { inputs, .. } => {
                    // Convert inputs to a vector of Expr
                    let input_exprs: Vec<Expr> =
                        inputs.iter().map(|e| e.as_ref().clone()).collect();
                    Self::collect_monotonic_id_calls(&input_exprs, result);
                }
                Expr::Agg(
                    AggExpr::Sum(expr)
                    | AggExpr::Min(expr)
                    | AggExpr::Max(expr)
                    | AggExpr::Mean(expr)
                    | AggExpr::Stddev(expr)
                    | AggExpr::Count(expr, _)
                    | AggExpr::CountDistinct(expr)
                    | AggExpr::ApproxCountDistinct(expr)
                    | AggExpr::BoolAnd(expr)
                    | AggExpr::BoolOr(expr)
                    | AggExpr::List(expr)
                    | AggExpr::Set(expr)
                    | AggExpr::Concat(expr)
                    | AggExpr::AnyValue(expr, _),
                ) => {
                    Self::collect_monotonic_id_calls(&[expr.as_ref().clone()], result);
                }
                Expr::ScalarFunction(function) => {
                    // Convert inputs to a vector of Expr
                    let input_exprs: Vec<Expr> =
                        function.inputs.iter().map(|e| e.as_ref().clone()).collect();
                    Self::collect_monotonic_id_calls(&input_exprs, result);
                }
                _ => {
                    // Other expression types don't contain sub-expressions to check
                }
            }
        }

        // Convert the HashSet to our Vec<(String, ExprRef)> format
        result
            .iter()
            .map(|expr| ("id".to_string(), ExprRef::from(expr.clone())))
            .collect()
    }

    /// Checks if the given expression contains the specified monotonic ID expression
    fn expression_contains_monotonic_id(expr: &ExprRef, monotonic_id_expr: &ExprRef) -> bool {
        // Simple equality check for now
        let expr_str = format!("{}", expr);
        let monotonic_id_str = format!("{}", monotonic_id_expr);
        expr_str.contains(&monotonic_id_str)
    }

    /// Simplified implementation - creates a new column reference expression to replace the monotonic ID
    fn create_new_aggregations(
        original_exprs: &[ExprRef],
        monotonic_ids: &[(String, ExprRef)],
    ) -> Vec<ExprRef> {
        let mut result = Vec::new();

        for expr in original_exprs {
            // Check if this expression contains a monotonic ID
            let mut expr_contains_monotonic_id = false;

            for (col_name, monotonic_id_expr) in monotonic_ids {
                if Self::expression_contains_monotonic_id(expr, monotonic_id_expr) {
                    expr_contains_monotonic_id = true;

                    // Create a new expression that uses the column reference instead of the function call
                    // but preserves the original aggregation operation and alias
                    let new_expr = match expr.as_ref() {
                        Expr::Alias(inner_expr, name) => {
                            if let Expr::Agg(agg) = inner_expr.as_ref() {
                                match agg {
                                    AggExpr::Sum(sum_expr) => {
                                        // For sum(monotonic_id + value), replace with sum(id + value)
                                        if let Expr::BinaryOp { op, left, right } =
                                            sum_expr.as_ref()
                                        {
                                            if Self::expression_contains_monotonic_id(
                                                left,
                                                monotonic_id_expr,
                                            ) {
                                                let new_left = Expr::Column(Column::Resolved(
                                                    ResolvedColumn::Basic(Arc::from(
                                                        col_name.clone(),
                                                    )),
                                                ));
                                                let new_sum_expr = Expr::BinaryOp {
                                                    op: *op,
                                                    left: Arc::new(new_left),
                                                    right: right.clone(),
                                                };
                                                let new_agg = AggExpr::Sum(Arc::new(new_sum_expr));
                                                Expr::Alias(
                                                    Arc::new(Expr::Agg(new_agg)),
                                                    name.clone(),
                                                )
                                            } else if Self::expression_contains_monotonic_id(
                                                right,
                                                monotonic_id_expr,
                                            ) {
                                                let new_right = Expr::Column(Column::Resolved(
                                                    ResolvedColumn::Basic(Arc::from(
                                                        col_name.clone(),
                                                    )),
                                                ));
                                                let new_sum_expr = Expr::BinaryOp {
                                                    op: *op,
                                                    left: left.clone(),
                                                    right: Arc::new(new_right),
                                                };
                                                let new_agg = AggExpr::Sum(Arc::new(new_sum_expr));
                                                Expr::Alias(
                                                    Arc::new(Expr::Agg(new_agg)),
                                                    name.clone(),
                                                )
                                            } else {
                                                expr.as_ref().clone()
                                            }
                                        } else if Self::expression_contains_monotonic_id(
                                            sum_expr,
                                            monotonic_id_expr,
                                        ) {
                                            // Direct replacement for sum(monotonic_id())
                                            let new_sum_expr = Expr::Column(Column::Resolved(
                                                ResolvedColumn::Basic(Arc::from(col_name.clone())),
                                            ));
                                            let new_agg = AggExpr::Sum(Arc::new(new_sum_expr));
                                            Expr::Alias(Arc::new(Expr::Agg(new_agg)), name.clone())
                                        } else {
                                            expr.as_ref().clone()
                                        }
                                    }
                                    AggExpr::Max(max_expr) => {
                                        // For max(monotonic_id()), replace with max(id)
                                        if Self::expression_contains_monotonic_id(
                                            max_expr,
                                            monotonic_id_expr,
                                        ) {
                                            let new_max_expr = Expr::Column(Column::Resolved(
                                                ResolvedColumn::Basic(Arc::from(col_name.clone())),
                                            ));
                                            let new_agg = AggExpr::Max(Arc::new(new_max_expr));
                                            Expr::Alias(Arc::new(Expr::Agg(new_agg)), name.clone())
                                        } else {
                                            expr.as_ref().clone()
                                        }
                                    }
                                    _ => expr.as_ref().clone(),
                                }
                            } else {
                                expr.as_ref().clone()
                            }
                        }
                        _ => expr.as_ref().clone(),
                    };

                    result.push(Arc::new(new_expr));
                    break;
                }
            }

            // If we didn't replace this expression, keep the original
            if !expr_contains_monotonic_id {
                result.push(expr.clone());
            }
        }

        result
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
                LogicalPlan::Filter(filter) => {
                    // Find all monotonically_increasing_id() calls in this Filter
                    let monotonic_ids = Self::find_monotonic_ids_in_filter(filter);

                    if !monotonic_ids.is_empty() {
                        // Check if ID column already exists in the input schema
                        let col_name = &monotonic_ids[0].0;
                        let schema = filter.input.schema();
                        let id_exists = schema.has_field(col_name);

                        // If it doesn't exist, add MonotonicallyIncreasingId op
                        let monotonic_plan = if !id_exists {
                            Self::create_monotonic_chain(
                                filter.input.clone(),
                                monotonic_ids.clone(),
                            )?
                        } else {
                            // Just use the input plan as is
                            filter.input.clone()
                        };

                        // Create a new filter predicate that replaces monotonic_id calls with column references
                        let new_predicate =
                            Self::create_new_filter_predicate(&filter.predicate, &monotonic_ids);

                        // Create a new Filter operation with the updated predicate
                        let final_plan = Arc::new(LogicalPlan::Filter(Filter::try_new(
                            monotonic_plan,
                            new_predicate,
                        )?));

                        Ok(Transformed::yes(final_plan))
                    } else {
                        Ok(Transformed::no(node))
                    }
                }
                LogicalPlan::Join(join) => {
                    // Find all monotonically_increasing_id() calls in join conditions
                    let monotonic_ids = Self::find_monotonic_ids_in_join(join);

                    if !monotonic_ids.is_empty() {
                        // We need to add monotonically_increasing_id operations to both left and right inputs
                        let left_monotonic_plan =
                            Self::create_monotonic_chain(join.left.clone(), monotonic_ids.clone())?;

                        let right_monotonic_plan = Self::create_monotonic_chain(
                            join.right.clone(),
                            monotonic_ids.clone(),
                        )?;

                        // Create new join conditions with column references
                        // The key here is to use the same ID column name for both sides
                        let new_left_on =
                            Self::create_new_join_conditions(&join.left_on, &monotonic_ids);

                        let new_right_on =
                            Self::create_new_join_conditions(&join.right_on, &monotonic_ids);

                        // Create a new Join operation with the updated inputs and conditions
                        let final_plan = Arc::new(LogicalPlan::Join(Join::try_new(
                            left_monotonic_plan,
                            right_monotonic_plan,
                            new_left_on,
                            new_right_on,
                            join.null_equals_nulls.clone(),
                            join.join_type,
                            join.join_strategy,
                        )?));

                        Ok(Transformed::yes(final_plan))
                    } else {
                        Ok(Transformed::no(node))
                    }
                }
                LogicalPlan::Aggregate(aggregate) => {
                    // Find all monotonically_increasing_id() calls in Aggregate
                    let monotonic_ids = Self::find_monotonic_ids_in_aggregate(aggregate);

                    if !monotonic_ids.is_empty() {
                        // Check if ID column already exists in the input schema
                        let col_name = &monotonic_ids[0].0;
                        let schema = aggregate.input.schema();
                        let id_exists = schema.has_field(col_name);

                        // First, add a MonotonicallyIncreasingId operation before the input if needed
                        let input_with_monotonic = if !id_exists {
                            Arc::new(LogicalPlan::MonotonicallyIncreasingId(
                                MonotonicallyIncreasingId::try_new(
                                    aggregate.input.clone(),
                                    Some(col_name),
                                )?,
                            ))
                        } else {
                            // Just use the input plan as is
                            aggregate.input.clone()
                        };

                        // Create new aggregation expressions with column references instead of function calls
                        let new_groupby =
                            Self::create_new_aggregations(&aggregate.groupby, &monotonic_ids);

                        let new_aggregations =
                            Self::create_new_aggregations(&aggregate.aggregations, &monotonic_ids);

                        // Create a new Aggregate operation with the updated expressions
                        let final_plan = Arc::new(LogicalPlan::Aggregate(Aggregate::try_new(
                            input_with_monotonic,
                            new_aggregations,
                            new_groupby,
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
