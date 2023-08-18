use std::collections::HashMap;

use super::expr::{AggExpr, Expr};

pub fn get_required_columns(e: &Expr) -> Vec<String> {
    // Returns all the column names required by this expression
    match e {
        Expr::Alias(child, _) => get_required_columns(child),
        Expr::Agg(agg) => match agg {
            AggExpr::Count(child, ..)
            | AggExpr::Sum(child)
            | AggExpr::Mean(child)
            | AggExpr::Min(child)
            | AggExpr::Max(child)
            | AggExpr::List(child)
            | AggExpr::Concat(child) => get_required_columns(child),
        },
        Expr::BinaryOp { left, right, .. } => {
            let mut req_cols = get_required_columns(left);
            req_cols.extend(get_required_columns(right));
            req_cols
        }
        Expr::Cast(child, _) => get_required_columns(child),
        Expr::Column(name) => vec![name.to_string()],
        Expr::Function { inputs, .. } => {
            let child_required_columns: Vec<Vec<String>> =
                inputs.iter().map(get_required_columns).collect();
            child_required_columns.concat()
        }
        Expr::Not(child) => get_required_columns(child),
        Expr::IsNull(child) => get_required_columns(child),
        Expr::Literal(_) => vec![],
        Expr::IfElse {
            if_true,
            if_false,
            predicate,
        } => {
            let mut req_cols = get_required_columns(if_true);
            req_cols.extend(get_required_columns(if_false));
            req_cols.extend(get_required_columns(predicate));
            req_cols
        }
    }
}

pub fn requires_computation(e: &Expr) -> bool {
    // Returns whether or not this expression runs any computation on the underlying data
    match e {
        Expr::Alias(child, _) => requires_computation(child),
        Expr::Column(..) | Expr::Literal(_) => false,
        Expr::Agg(..)
        | Expr::BinaryOp { .. }
        | Expr::Cast(..)
        | Expr::Function { .. }
        | Expr::Not(..)
        | Expr::IsNull(..)
        | Expr::IfElse { .. } => true,
    }
}

pub fn replace_columns_with_expressions(expr: &Expr, replace_map: &HashMap<String, Expr>) -> Expr {
    // Constructs a new deep-copied Expr which is `expr` but with all occurrences of Column(column_name) recursively
    // replaced with `new_expr` for all column_name -> new_expr mappings in replace_map.
    match expr {
        // BASE CASE: found a matching column
        Expr::Column(name) => match replace_map.get(&name.to_string()) {
            Some(replacement) => replacement.clone(),
            None => expr.clone(),
        },

        // BASE CASE: reached non-matching leaf node
        Expr::Literal(_) => expr.clone(),

        // RECURSIVE CASE: recursively replace for matching column
        Expr::Alias(child, name) => Expr::Alias(
            replace_columns_with_expressions(child, replace_map).into(),
            (*name).clone(),
        ),
        Expr::Agg(agg) => match agg {
            AggExpr::Count(child, mode) => Expr::Agg(AggExpr::Count(
                replace_columns_with_expressions(child, replace_map).into(),
                *mode,
            )),
            AggExpr::Sum(child) => Expr::Agg(AggExpr::Sum(
                replace_columns_with_expressions(child, replace_map).into(),
            )),
            AggExpr::Mean(child) => Expr::Agg(AggExpr::Mean(
                replace_columns_with_expressions(child, replace_map).into(),
            )),
            AggExpr::Min(child) => Expr::Agg(AggExpr::Min(
                replace_columns_with_expressions(child, replace_map).into(),
            )),
            AggExpr::Max(child) => Expr::Agg(AggExpr::Max(
                replace_columns_with_expressions(child, replace_map).into(),
            )),
            AggExpr::List(child) => Expr::Agg(AggExpr::List(
                replace_columns_with_expressions(child, replace_map).into(),
            )),
            AggExpr::Concat(child) => Expr::Agg(AggExpr::List(
                replace_columns_with_expressions(child, replace_map).into(),
            )),
        },
        Expr::BinaryOp { left, right, op } => Expr::BinaryOp {
            op: *op,
            left: replace_columns_with_expressions(left, replace_map).into(),
            right: replace_columns_with_expressions(right, replace_map).into(),
        },
        Expr::Cast(child, name) => Expr::Cast(
            replace_columns_with_expressions(child, replace_map).into(),
            (*name).clone(),
        ),
        Expr::Function { inputs, func } => Expr::Function {
            func: func.clone(),
            inputs: inputs
                .iter()
                .map(|e| replace_columns_with_expressions(e, replace_map))
                .collect(),
        },
        Expr::Not(child) => replace_columns_with_expressions(child, replace_map),
        Expr::IsNull(child) => replace_columns_with_expressions(child, replace_map),
        Expr::IfElse {
            if_true,
            if_false,
            predicate,
        } => Expr::IfElse {
            if_true: replace_columns_with_expressions(if_true, replace_map).into(),
            if_false: replace_columns_with_expressions(if_false, replace_map).into(),
            predicate: replace_columns_with_expressions(predicate, replace_map).into(),
        },
    }
}

pub fn replace_column_with_expression(expr: &Expr, column_name: &str, new_expr: &Expr) -> Expr {
    // Constructs a new deep-copied Expr which is `expr` but with all occurrences of Column(column_name) recursively
    // replaced with `new_expr`
    match expr {
        // BASE CASE: found a matching column
        Expr::Column(name) if name.as_ref().eq(column_name) => new_expr.clone(),

        // BASE CASE: reached non-matching leaf node
        Expr::Column(..) => expr.clone(),
        Expr::Literal(_) => expr.clone(),

        // RECURSIVE CASE: recursively replace for matching column
        Expr::Alias(child, name) => Expr::Alias(
            replace_column_with_expression(child, column_name, new_expr).into(),
            (*name).clone(),
        ),
        Expr::Agg(agg) => match agg {
            AggExpr::Count(child, mode) => Expr::Agg(AggExpr::Count(
                replace_column_with_expression(child, column_name, new_expr).into(),
                *mode,
            )),
            AggExpr::Sum(child) => Expr::Agg(AggExpr::Sum(
                replace_column_with_expression(child, column_name, new_expr).into(),
            )),
            AggExpr::Mean(child) => Expr::Agg(AggExpr::Mean(
                replace_column_with_expression(child, column_name, new_expr).into(),
            )),
            AggExpr::Min(child) => Expr::Agg(AggExpr::Min(
                replace_column_with_expression(child, column_name, new_expr).into(),
            )),
            AggExpr::Max(child) => Expr::Agg(AggExpr::Max(
                replace_column_with_expression(child, column_name, new_expr).into(),
            )),
            AggExpr::List(child) => Expr::Agg(AggExpr::List(
                replace_column_with_expression(child, column_name, new_expr).into(),
            )),
            AggExpr::Concat(child) => Expr::Agg(AggExpr::List(
                replace_column_with_expression(child, column_name, new_expr).into(),
            )),
        },
        Expr::BinaryOp { left, right, op } => Expr::BinaryOp {
            op: *op,
            left: replace_column_with_expression(left, column_name, new_expr).into(),
            right: replace_column_with_expression(right, column_name, new_expr).into(),
        },
        Expr::Cast(child, name) => Expr::Cast(
            replace_column_with_expression(child, column_name, new_expr).into(),
            (*name).clone(),
        ),
        Expr::Function { inputs, func } => Expr::Function {
            func: func.clone(),
            inputs: inputs
                .iter()
                .map(|e| replace_column_with_expression(e, column_name, new_expr))
                .collect(),
        },
        Expr::Not(child) => replace_column_with_expression(child, column_name, new_expr),
        Expr::IsNull(child) => replace_column_with_expression(child, column_name, new_expr),
        Expr::IfElse {
            if_true,
            if_false,
            predicate,
        } => Expr::IfElse {
            if_true: replace_column_with_expression(if_true, column_name, new_expr).into(),
            if_false: replace_column_with_expression(if_false, column_name, new_expr).into(),
            predicate: replace_column_with_expression(predicate, column_name, new_expr).into(),
        },
    }
}
