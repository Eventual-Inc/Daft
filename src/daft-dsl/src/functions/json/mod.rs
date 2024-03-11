mod query;

use query::JsonQueryEvaluator;
use serde::{Deserialize, Serialize};

use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum JsonExpr {
    Query(String),
}

impl JsonExpr {
    #[inline]
    pub fn query_evaluator(&self) -> &dyn FunctionEvaluator {
        use JsonExpr::*;
        match self {
            Query(_) => &JsonQueryEvaluator {},
        }
    }
}

pub fn query(input: &Expr, query: &str) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Json(JsonExpr::Query(query.to_string())),
        inputs: vec![input.clone()],
    }
}
