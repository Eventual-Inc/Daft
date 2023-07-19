mod contains;
mod explode;
mod join;
mod lengths;

use contains::ContainsEvaluator;
use explode::ExplodeEvaluator;
use join::JoinEvaluator;
use lengths::LengthsEvaluator;
use serde::{Deserialize, Serialize};

use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ListExpr {
    Explode,
    Join,
    Lengths,
    Contains,
}

impl ListExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ListExpr::*;
        match self {
            Explode => &ExplodeEvaluator {},
            Join => &JoinEvaluator {},
            Lengths => &LengthsEvaluator {},
            Contains => &ContainsEvaluator {},
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

pub fn lengths(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Lengths),
        inputs: vec![input.clone()],
    }
}

pub fn contains(input: &Expr, element: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Contains),
        inputs: vec![input.clone(), element.clone()],
    }
}
