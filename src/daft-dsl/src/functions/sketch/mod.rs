mod percentile;

use percentile::PercentileEvaluator;

use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SketchExpr {
    Percentile(Vec<[u8; 8]>),
}

impl SketchExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use SketchExpr::*;
        match self {
            Percentile(_) => &PercentileEvaluator {},
        }
    }
}

pub fn sketch_percentile(input: ExprRef, percentiles: &[f64]) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Sketch(SketchExpr::Percentile(
            percentiles.iter().map(|&p| p.to_be_bytes()).collect(),
        )),
        inputs: vec![input.clone()],
    }
    .into()
}
