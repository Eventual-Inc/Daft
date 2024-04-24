mod crop;
mod decode;
mod encode;
mod resize;

use crop::CropEvaluator;
use decode::DecodeEvaluator;
use encode::EncodeEvaluator;
use resize::ResizeEvaluator;
use serde::{Deserialize, Serialize};

use daft_core::datatypes::ImageFormat;

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ImageExpr {
    Decode { raise_error_on_failure: bool },
    Encode { image_format: ImageFormat },
    Resize { w: u32, h: u32 },
    Crop(),
}

impl ImageExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use ImageExpr::*;

        match self {
            Decode { .. } => &DecodeEvaluator {},
            Encode { .. } => &EncodeEvaluator {},
            Resize { .. } => &ResizeEvaluator {},
            Crop { .. } => &CropEvaluator {},
        }
    }
}

pub fn decode(input: ExprRef, raise_error_on_failure: bool) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Decode {
            raise_error_on_failure,
        }),
        inputs: vec![input],
    }
    .into()
}

pub fn encode(input: ExprRef, image_format: ImageFormat) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Encode { image_format }),
        inputs: vec![input],
    }
    .into()
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
