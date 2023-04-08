mod is_nan;

use is_nan::IsNanEvaluator;
use serde::{Deserialize, Serialize};

use crate::dsl::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FloatExpr {
    IsNan,
}

impl FloatExpr {
    #[inline]
    pub fn get_evaluator(&self) -> Box<dyn FunctionEvaluator> {
        use FloatExpr::*;
        match self {
            IsNan => Box::new(IsNanEvaluator {}),
        }
    }
}

pub fn is_nan(data: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Float(FloatExpr::IsNan),
        inputs: vec![data.clone()],
    }
}
