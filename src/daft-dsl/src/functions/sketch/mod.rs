mod percentile;

use percentile::PercentileEvaluator;

use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HashableVecPercentiles(pub Vec<f64>);

impl std::hash::Hash for HashableVecPercentiles {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0
            .iter()
            .for_each(|p| p.to_be_bytes().iter().for_each(|&b| state.write_u8(b)))
    }
}

impl Eq for HashableVecPercentiles {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SketchExpr {
    Percentile {
        percentiles: HashableVecPercentiles,
        force_list_output: bool,
    },
}

impl SketchExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use SketchExpr::*;
        match self {
            Percentile { .. } => &PercentileEvaluator {},
        }
    }
}

pub fn sketch_percentile(input: ExprRef, percentiles: &[f64], force_list_output: bool) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Sketch(SketchExpr::Percentile {
            percentiles: HashableVecPercentiles(percentiles.to_vec()),
            force_list_output,
        }),
        inputs: vec![input.clone()],
    }
    .into()
}
