mod get;

use get::GetEvaluator;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MapExpr {
    Get,
}

impl MapExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use MapExpr::*;
        match self {
            Get => &GetEvaluator {},
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
