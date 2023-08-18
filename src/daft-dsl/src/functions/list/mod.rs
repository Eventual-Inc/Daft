mod explode;
mod join;
mod lengths;

use explode::ExplodeEvaluator;
use join::JoinEvaluator;
use lengths::LengthsEvaluator;
use serde::{Deserialize, Serialize};

use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ListExpr {
    Explode,
    Join,
    Lengths,
}

impl ListExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ListExpr::*;
        match self {
            Explode => &ExplodeEvaluator {},
            Join => &JoinEvaluator {},
            Lengths => &LengthsEvaluator {},
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
