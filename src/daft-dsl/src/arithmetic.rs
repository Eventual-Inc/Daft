use std::ops::*;

use crate::{Expr, Operator};

macro_rules! impl_expr_op {
    ($math_op:ident, $func_name:ident, $op_name: ident) => {
        impl $math_op for &Expr {
            type Output = Expr;
            fn $func_name(self, rhs: Self) -> Self::Output {
                Expr::BinaryOp {
                    op: Operator::$op_name,
                    left: self.clone().into(),
                    right: rhs.clone().into(),
                }
            }
        }

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
impl_expr_op!(Div, div, TrueDivide);
impl_expr_op!(Rem, rem, Modulus);

#[cfg(test)]
mod tests {
    use crate::{col, Expr};
    use common_error::{DaftError, DaftResult};

    #[test]
    fn check_add_expr_type() -> DaftResult<()> {
        let a = col("a");
        let b = col("b");
        let c = a + b;
        match c {
            Expr::BinaryOp { .. } => Ok(()),
            other => Err(DaftError::ValueError(format!(
                "expected expression to be a binary op expression, got {other:?}"
            ))),
        }
    }
}
