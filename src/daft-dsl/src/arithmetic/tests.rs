use common_error::{DaftError, DaftResult};

use crate::{Expr, resolved_col};

#[test]
fn check_add_expr_type() -> DaftResult<()> {
    let a = resolved_col("a");
    let b = resolved_col("b");
    let c = a.add(b);
    match c.as_ref() {
        Expr::BinaryOp { .. } => Ok(()),
        other => Err(DaftError::ValueError(format!(
            "expected expression to be a binary op expression, got {other:?}"
        ))),
    }
}
