mod abs;
mod ceil;
mod exp;
mod floor;
mod log;
mod round;
mod sign;
mod sqrt;
mod trigonometry;

use abs::AbsEvaluator;
use ceil::CeilEvaluator;
use floor::FloorEvaluator;
use log::LogEvaluator;
use round::RoundEvaluator;
use serde::{Deserialize, Serialize};
use sign::SignEvaluator;
use sqrt::SqrtEvaluator;
use std::hash::Hash;
use trigonometry::Atan2Evaluator;

use crate::functions::numeric::exp::ExpEvaluator;
use crate::functions::numeric::trigonometry::{TrigonometricFunction, TrigonometryEvaluator};
use crate::{Expr, ExprRef};
use daft_core::utils::hashable_float_wrapper::FloatWrapper;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NumericExpr {
    Abs,
    Ceil,
    Floor,
    Sign,
    Round(i32),
    Sqrt,
    Sin,
    Cos,
    Tan,
    Cot,
    ArcSin,
    ArcCos,
    ArcTan,
    ArcTan2,
    Radians,
    Degrees,
    Log2,
    Log10,
    Log(FloatWrapper<f64>),
    Ln,
    Exp,
    ArcTanh,
    ArcCosh,
    ArcSinh,
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
            Sqrt => &SqrtEvaluator {},
            Sin => &TrigonometryEvaluator(TrigonometricFunction::Sin),
            Cos => &TrigonometryEvaluator(TrigonometricFunction::Cos),
            Tan => &TrigonometryEvaluator(TrigonometricFunction::Tan),
            Cot => &TrigonometryEvaluator(TrigonometricFunction::Cot),
            ArcSin => &TrigonometryEvaluator(TrigonometricFunction::ArcSin),
            ArcCos => &TrigonometryEvaluator(TrigonometricFunction::ArcCos),
            ArcTan => &TrigonometryEvaluator(TrigonometricFunction::ArcTan),
            ArcTan2 => &Atan2Evaluator {},
            Radians => &TrigonometryEvaluator(TrigonometricFunction::Radians),
            Degrees => &TrigonometryEvaluator(TrigonometricFunction::Degrees),
            Log2 => &LogEvaluator(log::LogFunction::Log2),
            Log10 => &LogEvaluator(log::LogFunction::Log10),
            Log(_) => &LogEvaluator(log::LogFunction::Log),
            Ln => &LogEvaluator(log::LogFunction::Ln),
            Exp => &ExpEvaluator {},
            ArcTanh => &TrigonometryEvaluator(TrigonometricFunction::ArcTanh),
            ArcCosh => &TrigonometryEvaluator(TrigonometricFunction::ArcCosh),
            ArcSinh => &TrigonometryEvaluator(TrigonometricFunction::ArcSinh),
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

pub fn sqrt(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Sqrt),
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

pub fn arctan2(input: ExprRef, other: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::ArcTan2),
        inputs: vec![input, other],
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

pub fn arctanh(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::ArcTanh),
        inputs: vec![input],
    }
    .into()
}

pub fn arccosh(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::ArcCosh),
        inputs: vec![input],
    }
    .into()
}

pub fn arcsinh(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::ArcSinh),
        inputs: vec![input],
    }
    .into()
}

pub fn log2(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Log2),
        inputs: vec![input],
    }
    .into()
}

pub fn log10(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Log10),
        inputs: vec![input],
    }
    .into()
}

pub fn log(input: ExprRef, base: f64) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Log(FloatWrapper(base))),
        inputs: vec![input],
    }
    .into()
}

pub fn ln(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Numeric(NumericExpr::Ln),
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
