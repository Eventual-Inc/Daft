use std::collections::HashMap;

use common_treenode::{Transformed, TreeNode, VisitRecursion};

use crate::Operator;

use super::expr::Expr;

pub fn get_required_columns(e: &Expr) -> Vec<String> {
    let mut cols = vec![];
    e.apply(&mut |expr| {
        if let Expr::Column(name) = expr {
            cols.push(name.as_ref().into());
        }
        Ok(VisitRecursion::Continue)
    })
    .expect("Error occurred when visiting for required columns");
    cols
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
        | Expr::NotNull(..)
        | Expr::IfElse { .. } => true,
    }
}

pub fn replace_columns_with_expressions(expr: &Expr, replace_map: &HashMap<String, Expr>) -> Expr {
    expr.clone()
        .transform(&|e| {
            if let Expr::Column(ref name) = e && let Some(tgt) = replace_map.get(name.as_ref()) {
                Ok(Transformed::Yes(tgt.clone()))
            } else {
                Ok(Transformed::No(e))
            }
        })
        .expect("Error occurred when rewriting column expressions")
}

pub fn split_conjuction(expr: &Expr) -> Vec<&Expr> {
    let mut splits = vec![];
    _split_conjuction(expr, &mut splits);
    splits
}

fn _split_conjuction<'a>(expr: &'a Expr, out_exprs: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            op: Operator::And,
            left,
            right,
        } => {
            _split_conjuction(left, out_exprs);
            _split_conjuction(right, out_exprs);
        }
        Expr::Alias(inner_expr, ..) => _split_conjuction(inner_expr, out_exprs),
        _ => {
            out_exprs.push(expr);
        }
    }
}

pub fn conjuct(exprs: Vec<Expr>) -> Option<Expr> {
    exprs.into_iter().reduce(|acc, expr| acc.and(&expr))
}
