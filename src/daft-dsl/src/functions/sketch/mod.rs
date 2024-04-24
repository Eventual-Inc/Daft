mod percentile;

use percentile::PercentileEvaluator;

use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SketchExpr {
    Percentile,
}

impl SketchExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use SketchExpr::*;
        match self {
            Percentile => &PercentileEvaluator {},
        }
    }
}

pub fn sketch_percentile(input: ExprRef, q: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Sketch(SketchExpr::Percentile),
        inputs: vec![input.clone(), q.clone()],
    }
    .into()
}
