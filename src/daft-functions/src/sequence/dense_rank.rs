use daft_dsl::{Expr, ExprRef, WindowExpr};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DenseRank {}

#[must_use]
pub fn dense_rank() -> ExprRef {
    Expr::WindowFunction(WindowExpr::DenseRank()).into()
}
