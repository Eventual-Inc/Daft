mod explode;
mod get;
mod join;
mod lengths;

use explode::ExplodeEvaluator;
use get::GetEvaluator;
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
    Get,
}

impl ListExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ListExpr::*;
        match self {
            Explode => &ExplodeEvaluator {},
            Join => &JoinEvaluator {},
            Lengths => &LengthsEvaluator {},
            Get => &GetEvaluator {},
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

pub fn get(input: &Expr, idx: &Expr, default: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Get),
        inputs: vec![input.clone(), idx.clone(), default.clone()],
    }
}
