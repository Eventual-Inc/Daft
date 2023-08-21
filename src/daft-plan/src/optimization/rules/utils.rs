use daft_dsl::{Expr, Operator};

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
