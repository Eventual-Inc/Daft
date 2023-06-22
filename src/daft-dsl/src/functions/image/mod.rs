mod decode;
mod encode;
mod resize;

use decode::DecodeEvaluator;
use encode::EncodeEvaluator;
use resize::ResizeEvaluator;
use serde::{Deserialize, Serialize};

use daft_core::{datatypes::ImageFormat};

use crate::Expr;


use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ImageExpr {
    Decode(),
    Encode { image_format: ImageFormat },
    Resize { w: u32, h: u32 },
}

impl ImageExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ImageExpr::*;

        match self {
            Decode() => &DecodeEvaluator {},
            Encode { .. } => &EncodeEvaluator {},
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

pub fn encode(input: &Expr, image_format: ImageFormat) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Encode { image_format }),
        inputs: vec![input.clone()],
    }
}

pub fn resize(input: &Expr, w: u32, h: u32) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Resize { w, h }),
        inputs: vec![input.clone()],
    }
}
