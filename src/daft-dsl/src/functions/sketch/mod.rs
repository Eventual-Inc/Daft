mod quantile;

use quantile::QuantileEvaluator;

use serde::{Deserialize, Serialize};

use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SketchExpr {
    Quantile,
}

impl SketchExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use SketchExpr::*;
        match self {
            Quantile => &QuantileEvaluator {},
        }
    }
}

pub fn approx_quantile(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Sketch(SketchExpr::Quantile),
        inputs: vec![input.clone()],
    }
}
