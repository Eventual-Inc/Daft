use daft_dsl::{Expr, ExprRef, WindowExpr};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RowNumber {}

#[must_use]
pub fn row_number() -> ExprRef {
    Expr::WindowFunction(WindowExpr::RowNumber).into()
}
