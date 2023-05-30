mod explode;

use explode::ExplodeEvaluator;
use serde::{Deserialize, Serialize};

use crate::dsl::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ListExpr {
    Explode,
}

impl ListExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ListExpr::*;
        match self {
            Explode => &ExplodeEvaluator {},
        }
    }
}

pub fn explode(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::List(ListExpr::Explode),
        inputs: vec![input.clone()],
    }
}
