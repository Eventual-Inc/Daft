use std::collections::HashMap;

use common_treenode::{Transformed, TreeNode, VisitRecursion};

use crate::{ExprRef, Operator};

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
        | Expr::FillNull(..)
        | Expr::IsIn { .. }
        | Expr::IfElse { .. } => true,
    }
}

pub fn replace_columns_with_expressions(
    expr: Expr,
    replace_map: &HashMap<String, ExprRef>,
) -> Expr {
    expr.transform(&|e| {
        if let Expr::Column(ref name) = e && let Some(tgt) = replace_map.get(name.as_ref()) {
                // work around until we get transforms that can run on ExprRef
                let tgt = tgt.as_ref().clone();
                Ok(Transformed::Yes(tgt.clone()))
            } else {
                Ok(Transformed::No(e))
            }
    })
    .expect("Error occurred when rewriting column expressions")
}

pub fn split_conjuction(expr: &ExprRef) -> Vec<&ExprRef> {
    let mut splits = vec![];
    _split_conjuction(expr, &mut splits);
    splits
}

fn _split_conjuction<'a>(expr: &'a ExprRef, out_exprs: &mut Vec<&'a ExprRef>) {
    match expr.as_ref() {
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

pub fn conjuct<T: IntoIterator<Item = ExprRef>>(exprs: T) -> Option<ExprRef> {
    exprs.into_iter().reduce(|acc, expr| acc.and(expr))
}
