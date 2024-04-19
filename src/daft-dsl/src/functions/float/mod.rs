mod is_nan;

use is_nan::IsNanEvaluator;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FloatExpr {
    IsNan,
}

impl FloatExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use FloatExpr::*;
        match self {
            IsNan => &IsNanEvaluator {},
        }
    }
}

pub fn is_nan(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Float(FloatExpr::IsNan),
        inputs: vec![data],
    }
    .into()
}
