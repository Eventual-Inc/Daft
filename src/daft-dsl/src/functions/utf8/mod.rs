mod contains;
mod endswith;
mod length;
mod startswith;

use contains::ContainsEvaluator;
use endswith::EndswithEvaluator;
use length::LengthEvaluator;
use serde::{Deserialize, Serialize};
use startswith::StartswithEvaluator;

use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Utf8Expr {
    EndsWith,
    StartsWith,
    Contains,
    Length,
}

impl Utf8Expr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use Utf8Expr::*;
        match self {
            EndsWith => &EndswithEvaluator {},
            StartsWith => &StartswithEvaluator {},
            Contains => &ContainsEvaluator {},
            Length => &LengthEvaluator {},
        }
    }
}

pub fn endswith(data: &Expr, pattern: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::EndsWith),
        inputs: vec![data.clone(), pattern.clone()],
    }
}

pub fn startswith(data: &Expr, pattern: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::StartsWith),
        inputs: vec![data.clone(), pattern.clone()],
    }
}

pub fn contains(data: &Expr, pattern: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Contains),
        inputs: vec![data.clone(), pattern.clone()],
    }
}

pub fn length(data: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Utf8(Utf8Expr::Length),
        inputs: vec![data.clone()],
    }
}
