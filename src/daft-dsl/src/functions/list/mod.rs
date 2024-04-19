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

use crate::{ExprRef, Expr};

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

pub fn explode(input: ExprRef) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Explode),
        inputs: vec![input],
    }
}

pub fn join(input: ExprRef, delimiter: ExprRef) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Join),
        inputs: vec![input, delimiter],
    }
}

pub fn count(input: ExprRef, mode: CountMode) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Count(mode)),
        inputs: vec![input],
    }
}

pub fn get(input: ExprRef, idx: ExprRef, default: ExprRef) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Get),
        inputs: vec![input, idx, default],
    }
}

pub fn sum(input: ExprRef) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Sum),
        inputs: vec![input],
    }
}

pub fn mean(input: ExprRef) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Mean),
        inputs: vec![input],
    }
}

pub fn min(input: ExprRef) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Min),
        inputs: vec![input],
    }
}

pub fn max(input: ExprRef) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Max),
        inputs: vec![input],
    }
}
