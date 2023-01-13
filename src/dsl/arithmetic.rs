use std::ops::*;

use crate::dsl::{Expr, Operator};

macro_rules! impl_expr_op {
    ($math_op:ident, $func_name:ident, $op_name: ident) => {
        impl $math_op for Expr {
            type Output = Expr;
            fn $func_name(self, rhs: Self) -> Self::Output {
                Expr::BinaryOp {
                    op: Operator::$op_name,
                    left: self.into(),
                    right: rhs.into(),
                }
            }
        }
    };
}

impl_expr_op!(Add, add, Plus);
impl_expr_op!(Sub, sub, Minus);
impl_expr_op!(Mul, mul, Multiply);
impl_expr_op!(Div, div, Divide);
impl_expr_op!(Rem, rem, Modulus);
