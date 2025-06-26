mod get;

use get::GetEvaluator;
use serde::{Deserialize, Serialize};

use super::FunctionEvaluator;
use crate::{Expr, ExprRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StructExpr {
    Get(String),
}

impl StructExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            Self::Get(_) => &GetEvaluator {},
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
