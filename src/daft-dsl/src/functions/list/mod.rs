mod get;
mod join;
mod max;
mod mean;
mod min;
mod slice;
mod sum;

use get::GetEvaluator;
use join::JoinEvaluator;
use max::MaxEvaluator;
use mean::MeanEvaluator;
use min::MinEvaluator;
use serde::{Deserialize, Serialize};
use slice::SliceEvaluator;
use sum::SumEvaluator;

use super::FunctionEvaluator;
use crate::{Expr, ExprRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ListExpr {
    Join,
    Get,
    Sum,
    Mean,
    Min,
    Max,
    Slice,
}

impl ListExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ListExpr::*;
        match self {
            Join => &JoinEvaluator {},
            Get => &GetEvaluator {},
            Sum => &SumEvaluator {},
            Mean => &MeanEvaluator {},
            Min => &MinEvaluator {},
            Max => &MaxEvaluator {},
            Slice => &SliceEvaluator {},
        }
    }
}

pub fn join(input: ExprRef, delimiter: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Join),
        inputs: vec![input, delimiter],
    }
    .into()
}

pub fn get(input: ExprRef, idx: ExprRef, default: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Get),
        inputs: vec![input, idx, default],
    }
    .into()
}

pub fn sum(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Sum),
        inputs: vec![input],
    }
    .into()
}

pub fn mean(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Mean),
        inputs: vec![input],
    }
    .into()
}

pub fn min(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Min),
        inputs: vec![input],
    }
    .into()
}

pub fn max(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Max),
        inputs: vec![input],
    }
    .into()
}

pub fn slice(input: ExprRef, start: ExprRef, end: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Slice),
        inputs: vec![input, start, end],
    }
    .into()
}
