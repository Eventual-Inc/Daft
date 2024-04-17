mod count;
mod explode;
mod get;
mod join;
mod max;
mod mean;
mod min;
mod sum;

use count::CountEvaluator;
use daft_core::CountMode;
use explode::ExplodeEvaluator;
use get::GetEvaluator;
use join::JoinEvaluator;
use max::MaxEvaluator;
use mean::MeanEvaluator;
use min::MinEvaluator;
use serde::{Deserialize, Serialize};
use sum::SumEvaluator;

use crate::Expr;

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
        }
    }
}

pub fn explode(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Explode),
        inputs: vec![input.clone()],
    }
}

pub fn join(input: &Expr, delimiter: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Join),
        inputs: vec![input.clone(), delimiter.clone()],
    }
}

pub fn count(input: &Expr, mode: CountMode) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Count(mode)),
        inputs: vec![input.clone()],
    }
}

pub fn get(input: &Expr, idx: &Expr, default: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Get),
        inputs: vec![input.clone(), idx.clone(), default.clone()],
    }
}

pub fn sum(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Sum),
        inputs: vec![input.clone()],
    }
}

pub fn mean(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Mean),
        inputs: vec![input.clone()],
    }
}

pub fn min(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Min),
        inputs: vec![input.clone()],
    }
}

pub fn max(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Max),
        inputs: vec![input.clone()],
    }
}
