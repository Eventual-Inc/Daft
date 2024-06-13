mod fill_nan;
mod is_inf;
mod is_nan;
mod not_nan;

use fill_nan::FillNanEvaluator;
use is_inf::IsInfEvaluator;
use is_nan::IsNanEvaluator;
use not_nan::NotNanEvaluator;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FloatExpr {
    IsNan,
    IsInf,
    NotNan,
    FillNan,
}

impl FloatExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use FloatExpr::*;
        match self {
            IsNan => &IsNanEvaluator {},
            IsInf => &IsInfEvaluator {},
            NotNan => &NotNanEvaluator {},
            FillNan => &FillNanEvaluator {},
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

pub fn is_inf(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Float(FloatExpr::IsInf),
        inputs: vec![data],
    }
    .into()
}

pub fn not_nan(data: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Float(FloatExpr::NotNan),
        inputs: vec![data],
    }
    .into()
}

pub fn fill_nan(data: ExprRef, fill_value: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Float(FloatExpr::FillNan),
        inputs: vec![data, fill_value],
    }
    .into()
}
