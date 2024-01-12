mod get;

use get::GetEvaluator;
use serde::{Deserialize, Serialize};

use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StructExpr {
    Get(String),
}

impl StructExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use StructExpr::*;
        match self {
            Get(_) => &GetEvaluator {},
        }
    }
}

pub fn get(input: &Expr, name: &str) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Struct(StructExpr::Get(name.to_string())),
        inputs: vec![input.clone()],
    }
}
