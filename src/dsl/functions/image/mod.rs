mod decode;
mod resize;

use decode::DecodeEvaluator;
use resize::ResizeEvaluator;
use serde::{Deserialize, Serialize};

use crate::dsl::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ImageExpr {
    Decode(),
    Resize { w: u32, h: u32 },
}

impl ImageExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ImageExpr::*;

        match self {
            Decode() => &DecodeEvaluator {},
            Resize { .. } => &ResizeEvaluator {},
        }
    }
}

pub fn decode(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Decode()),
        inputs: vec![input.clone()],
    }
}

pub fn resize(input: &Expr, w: u32, h: u32) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Resize { w, h }),
        inputs: vec![input.clone()],
    }
}
