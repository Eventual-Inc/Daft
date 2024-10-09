#[cfg(test)]
mod tests;

use crate::{Expr, ExprRef, Operator};

macro_rules! impl_expr_op {
    ($func_name:ident, $op_name: ident) => {
        impl Expr {
            #[allow(clippy::should_implement_trait)]
            pub fn $func_name(self: ExprRef, rhs: ExprRef) -> ExprRef {
                Expr::BinaryOp {
                    op: Operator::$op_name,
                    left: self,
                    right: rhs,
                }
                .into()
            }
        }
    };
}

impl_expr_op!(add, Plus);
impl_expr_op!(sub, Minus);
impl_expr_op!(mul, Multiply);
impl_expr_op!(div, TrueDivide);
impl_expr_op!(rem, Modulus);
