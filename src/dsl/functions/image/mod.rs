mod decode;

use decode::DecodeEvaluator;
use serde::{Deserialize, Serialize};

use crate::dsl::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ImageExpr {
    Decode(),
}

impl ImageExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ImageExpr::*;

        match self {
            Decode() => &DecodeEvaluator {},
        }
    }
}

pub fn decode(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Decode()),
        inputs: vec![input.clone()],
    }
}
