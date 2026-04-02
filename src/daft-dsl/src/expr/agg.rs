use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::{AggExpr, Expr, ExprRef};

pub fn extract_agg_expr(expr: &ExprRef) -> DaftResult<(AggExpr, Option<Arc<str>>)> {
    match expr.as_ref() {
        Expr::Agg(agg_expr) => Ok((agg_expr.clone(), None)),
        Expr::Alias(e, name) => {
            extract_agg_expr(e).map(|(agg_expr, _)| (agg_expr, Some(name.clone())))
        }
        _ => Err(DaftError::InternalError(format!(
            "Expected non-agg expressions in aggregation to be factored out before plan translation. Got: {:?}",
            expr
        ))),
    }
}
