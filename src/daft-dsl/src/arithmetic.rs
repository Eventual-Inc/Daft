use std::ops::*;

use crate::{Expr, Operator};
use crate::ExprRef;

macro_rules! impl_expr_op {
    ($func_name:ident, $op_name: ident) => {
    impl Expr {
        pub fn $func_name(self: ExprRef, rhs: ExprRef) -> Expr {
            Expr::BinaryOp {
                op: Operator::$op_name,
                left: self,
                right: rhs,
            }

        }
    }
    };
}

impl_expr_op!(add, Plus);
impl_expr_op!(sub, Minus);
impl_expr_op!(mul, Multiply);
impl_expr_op!(div, TrueDivide);
impl_expr_op!(rem, Modulus);

#[cfg(test)]
mod tests {
    use crate::{col, Expr};
    use common_error::{DaftError, DaftResult};

    #[test]
    fn check_add_expr_type() -> DaftResult<()> {
        let a = col("a");
        let b = col("b");
        let c = a.add(b);
        match c {
            Expr::BinaryOp { .. } => Ok(()),
            other => Err(DaftError::ValueError(format!(
                "expected expression to be a binary op expression, got {other:?}"
            ))),
        }
    }
}
