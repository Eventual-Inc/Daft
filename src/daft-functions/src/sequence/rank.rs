use daft_dsl::{Expr, ExprRef, WindowExpr};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Rank {}

#[must_use]
pub fn rank() -> ExprRef {
    Expr::WindowFunction(WindowExpr::Rank()).into()
}
