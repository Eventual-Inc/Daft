use std::collections::HashMap;

use common_error::DaftResult;
use common_treenode::{TreeNode, TreeNodeVisitor, VisitRecursion};

use super::expr::{AggExpr, Expr};

struct RequiredColumnVisitor {
    required: Vec<String>,
}

impl TreeNodeVisitor for RequiredColumnVisitor {
    type N = Expr;
    fn pre_visit(&mut self, node: &Self::N) -> DaftResult<VisitRecursion> {
        if let Expr::Column(name) = node {
            self.required.push(name.as_ref().into());
        };
        Ok(VisitRecursion::Continue)
    }
}

pub fn get_required_columns(e: &Expr) -> Vec<String> {
    let mut visitor = RequiredColumnVisitor { required: vec![] };
    e.visit(&mut visitor)
        .expect("Error occurred when visiting for required columns");
    visitor.required
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
