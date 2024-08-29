mod crop;
mod resize;
mod to_mode;

use crop::CropEvaluator;
use resize::ResizeEvaluator;
use serde::{Deserialize, Serialize};

use daft_core::datatypes::ImageMode;
use to_mode::ToModeEvaluator;

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ImageExpr {
    Resize { w: u32, h: u32 },
    Crop(),
    ToMode { mode: ImageMode },
}

impl ImageExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ImageExpr::*;

        match self {
            Resize { .. } => &ResizeEvaluator {},
            Crop { .. } => &CropEvaluator {},
            ToMode { .. } => &ToModeEvaluator {},
        }
    }
}

pub fn resize(input: ExprRef, w: u32, h: u32) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Resize { w, h }),
        inputs: vec![input],
    }
    .into()
}

pub fn crop(input: ExprRef, bbox: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Crop()),
        inputs: vec![input, bbox],
    }
    .into()
}

pub fn to_mode(input: ExprRef, mode: ImageMode) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::ToMode { mode }),
        inputs: vec![input],
    }
    .into()
}
