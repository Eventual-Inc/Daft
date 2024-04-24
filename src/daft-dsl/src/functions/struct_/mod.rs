mod get;

use get::GetEvaluator;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

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

pub fn get(input: ExprRef, name: &str) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Struct(StructExpr::Get(name.to_string())),
        inputs: vec![input],
    }
    .into()
}
