mod abs;
mod ceil;
mod exp;
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

use crate::functions::numeric::exp::ExpEvaluator;
use crate::functions::numeric::trigonometry::{TrigonometricFunction, TrigonometryEvaluator};
use crate::{Expr, ExprRef};

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
    Cot,
    ArcSin,
    ArcCos,
    ArcTan,
    Radians,
    Degrees,
    Exp,
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
            Cot => &TrigonometryEvaluator(TrigonometricFunction::Cot),
            ArcSin => &TrigonometryEvaluator(TrigonometricFunction::ArcSin),
            ArcCos => &TrigonometryEvaluator(TrigonometricFunction::ArcCos),
            ArcTan => &TrigonometryEvaluator(TrigonometricFunction::ArcTan),
            Radians => &TrigonometryEvaluator(TrigonometricFunction::Radians),
            Degrees => &TrigonometryEvaluator(TrigonometricFunction::Degrees),
            Exp => &ExpEvaluator {},
        }
    }
}

pub fn abs(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Abs),
        inputs: vec![input],
    }
    .into()
}

pub fn ceil(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Ceil),
        inputs: vec![input],
    }
    .into()
}

pub fn floor(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Floor),
        inputs: vec![input],
    }
    .into()
}

pub fn sign(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Sign),
        inputs: vec![input],
    }
    .into()
}

pub fn round(input: ExprRef, decimal: i32) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Round(decimal)),
        inputs: vec![input],
    }
    .into()
}

pub fn sin(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Sin),
        inputs: vec![input],
    }
    .into()
}

pub fn cos(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Cos),
        inputs: vec![input],
    }
    .into()
}

pub fn tan(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Tan),
        inputs: vec![input],
    }
    .into()
}

pub fn cot(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Cot),
        inputs: vec![input],
    }
    .into()
}

pub fn arcsin(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::ArcSin),
        inputs: vec![input],
    }
    .into()
}

pub fn arccos(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::ArcCos),
        inputs: vec![input],
    }
    .into()
}

pub fn arctan(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::ArcTan),
        inputs: vec![input],
    }
    .into()
}

pub fn radians(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Radians),
        inputs: vec![input],
    }
    .into()
}

pub fn degrees(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Degrees),
        inputs: vec![input],
    }
    .into()
}

pub fn exp(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Exp),
        inputs: vec![input],
    }
    .into()
}
