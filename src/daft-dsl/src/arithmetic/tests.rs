use common_error::{DaftError, DaftResult};

use crate::{col, Expr};

#[test]
fn check_add_expr_type() -> DaftResult<()> {
    let a = col("a");
    let b = col("b");
    let c = a.add(b);
    match c.as_ref() {
        Expr::BinaryOp { .. } => Ok(()),
        other => Err(DaftError::ValueError(format!(
            "expected expression to be a binary op expression, got {other:?}"
        ))),
    }
}
