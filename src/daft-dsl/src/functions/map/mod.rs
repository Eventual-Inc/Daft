mod get;

use get::GetEvaluator;
use serde::{Deserialize, Serialize};

use super::FunctionEvaluator;
use crate::{Expr, ExprRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MapExpr {
    Get,
}

impl MapExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            Self::Get => &GetEvaluator {},
        }
    }
}

pub fn get(input: ExprRef, key: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Map(MapExpr::Get),
        inputs: vec![input, key],
    }
    .into()
}
