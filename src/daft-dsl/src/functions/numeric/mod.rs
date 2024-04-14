mod abs;
mod ceil;
mod floor;
mod round;
mod sign;
mod trigonometry;

use abs::AbsEvaluator;
use ceil::CeilEvaluator;
use floor::FloorEvaluator;
use round::RoundEvaluator;
use sign::SignEvaluator;

use serde::{Deserialize, Serialize};

use crate::functions::numeric::trigonometry::{TrigonometricFunction, TrigonometryEvaluator};
use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NumericExpr {
    Abs,
    Ceil,
    Floor,
    Sign,
    Round(i32),
    Sin,
    Cos,
    Tan,
}

impl NumericExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use NumericExpr::*;
        match self {
            Abs => &AbsEvaluator {},
            Ceil => &CeilEvaluator {},
            Floor => &FloorEvaluator {},
            Sign => &SignEvaluator {},
            Round(_) => &RoundEvaluator {},
            Sin => &TrigonometryEvaluator(TrigonometricFunction::Sin),
            Cos => &TrigonometryEvaluator(TrigonometricFunction::Cos),
            Tan => &TrigonometryEvaluator(TrigonometricFunction::Tan),
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

pub fn floor(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Floor),
        inputs: vec![input.clone()],
    }
}

pub fn sign(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Sign),
        inputs: vec![input.clone()],
    }
}

pub fn round(input: &Expr, decimal: i32) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Round(decimal)),
        inputs: vec![input.clone()],
    }
}

pub fn sin(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Sin),
        inputs: vec![input.clone()],
    }
}

pub fn cos(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Cos),
        inputs: vec![input.clone()],
    }
}

pub fn tan(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Tan),
        inputs: vec![input.clone()],
    }
}
