mod chunk;
mod count;
mod explode;
mod get;
mod join;
mod max;
mod mean;
mod min;
mod slice;
mod sum;

use chunk::ChunkEvaluator;
use count::CountEvaluator;
use daft_core::CountMode;
use explode::ExplodeEvaluator;
use get::GetEvaluator;
use join::JoinEvaluator;
use max::MaxEvaluator;
use mean::MeanEvaluator;
use min::MinEvaluator;
use serde::{Deserialize, Serialize};
use slice::SliceEvaluator;
use sum::SumEvaluator;

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ListExpr {
    Explode,
    Join,
    Count(CountMode),
    Get,
    Sum,
    Mean,
    Min,
    Max,
    Slice,
    Chunk(usize),
}

impl ListExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ListExpr::*;
        match self {
            Explode => &ExplodeEvaluator {},
            Join => &JoinEvaluator {},
            Count(_) => &CountEvaluator {},
            Get => &GetEvaluator {},
            Sum => &SumEvaluator {},
            Mean => &MeanEvaluator {},
            Min => &MinEvaluator {},
            Max => &MaxEvaluator {},
            Slice => &SliceEvaluator {},
            Chunk(_) => &ChunkEvaluator {},
        }
    }
}

pub fn explode(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Explode),
        inputs: vec![input],
    }
    .into()
}

pub fn join(input: ExprRef, delimiter: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Join),
        inputs: vec![input, delimiter],
    }
    .into()
}

pub fn count(input: ExprRef, mode: CountMode) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Count(mode)),
        inputs: vec![input],
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

pub fn chunk(input: ExprRef, size: usize) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Chunk(size)),
        inputs: vec![input],
    }
    .into()
}
