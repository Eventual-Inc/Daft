mod crop;
mod to_mode;

use crop::CropEvaluator;
use serde::{Deserialize, Serialize};

use daft_core::datatypes::ImageMode;
use to_mode::ToModeEvaluator;

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ImageExpr {
    Crop(),
    ToMode { mode: ImageMode },
}

impl ImageExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ImageExpr::*;

        match self {
            Crop { .. } => &CropEvaluator {},
            ToMode { .. } => &ToModeEvaluator {},
        }
    }
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
