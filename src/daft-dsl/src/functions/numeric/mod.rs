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
use std::hash::{Hash, Hasher};

use crate::functions::numeric::exp::ExpEvaluator;
use crate::functions::numeric::trigonometry::{TrigonometricFunction, TrigonometryEvaluator};
use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

// Wrapper struct to implement Eq and Hash traits for f64.
// This is necessary to use f64 as a key in a HashMap.
// The Log enum in the NumericExpr enum uses this wrapper to store the base value for the log function.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct F64Wrapper(f64);

impl F64Wrapper {
    // Method to get the inner f64 value
    fn get_value(&self) -> f64 {
        self.0
    }
}

impl Eq for F64Wrapper {}
impl Hash for F64Wrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use the to_bits method to convert the f64 into its raw bytes representation
        self.0.to_bits().hash(state);
    }
}

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
    Radians,
    Degrees,
    Log2,
    Log10,
    Log(F64Wrapper),
    Ln,
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
            Sqrt => &SqrtEvaluator {},
            Sin => &TrigonometryEvaluator(TrigonometricFunction::Sin),
            Cos => &TrigonometryEvaluator(TrigonometricFunction::Cos),
            Tan => &TrigonometryEvaluator(TrigonometricFunction::Tan),
            Cot => &TrigonometryEvaluator(TrigonometricFunction::Cot),
            ArcSin => &TrigonometryEvaluator(TrigonometricFunction::ArcSin),
            ArcCos => &TrigonometryEvaluator(TrigonometricFunction::ArcCos),
            ArcTan => &TrigonometryEvaluator(TrigonometricFunction::ArcTan),
            Radians => &TrigonometryEvaluator(TrigonometricFunction::Radians),
            Degrees => &TrigonometryEvaluator(TrigonometricFunction::Degrees),
            Log2 => &LogEvaluator(log::LogFunction::Log2),
            Log10 => &LogEvaluator(log::LogFunction::Log10),
            Log(_) => &LogEvaluator(log::LogFunction::Log),
            Ln => &LogEvaluator(log::LogFunction::Ln),
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
        func: super::FunctionExpr::Numeric(NumericExpr::Log(F64Wrapper(base))),
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
