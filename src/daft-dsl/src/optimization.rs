use std::collections::HashMap;

use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};

use crate::{ExprRef, Operator};

use super::expr::Expr;

pub fn get_required_columns(e: &ExprRef) -> Vec<String> {
    let mut cols = vec![];
    e.apply(&mut |expr: &ExprRef| {
        if let Expr::Column(ref name) = &**expr {
            cols.push(name.to_string());
        }
        Ok(TreeNodeRecursion::Continue)
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
        | Expr::ScalarFunction { .. }
        | Expr::Not(..)
        | Expr::IsNull(..)
        | Expr::NotNull(..)
        | Expr::FillNull(..)
        | Expr::IsIn { .. }
        | Expr::Between { .. }
        | Expr::IfElse { .. } => true,
    }
}

pub fn replace_columns_with_expressions(
    expr: ExprRef,
    replace_map: &HashMap<String, ExprRef>,
) -> ExprRef {
    let transformed = expr
        .transform(&|e: ExprRef| {
            if let Expr::Column(ref name) = e.as_ref()
                && let Some(tgt) = replace_map.get(name.as_ref())
            {
                Ok(Transformed::yes(tgt.clone()))
            } else {
                Ok(Transformed::no(e))
            }
        })
        .expect("Error occurred when rewriting column expressions");
    transformed.data
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
