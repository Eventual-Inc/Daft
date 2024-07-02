mod query;

use query::JsonQueryEvaluator;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum JsonExpr {
    Query(String),
}

impl JsonExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use JsonExpr::*;
        match self {
            Query(_) => &JsonQueryEvaluator {},
        }
    }
}

pub fn query(input: ExprRef, query: &str) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Json(JsonExpr::Query(query.to_string())),
        inputs: vec![input],
    }
    .into()
}
