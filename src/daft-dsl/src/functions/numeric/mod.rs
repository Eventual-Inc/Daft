mod abs;
mod ceil;

use abs::AbsEvaluator;
use ceil::CeilEvaluator;

use serde::{Deserialize, Serialize};

use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NumericExpr {
    Abs,
    Ceil,
}

impl NumericExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use NumericExpr::*;
        match self {
            Abs => &AbsEvaluator {},
            Ceil => &CeilEvaluator {},
        }
    }
}

pub fn abs(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Abs),
        inputs: vec![input.clone()],
    }
}

pub fn ceil(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Ceil),
        inputs: vec![input.clone()],
    }
}
