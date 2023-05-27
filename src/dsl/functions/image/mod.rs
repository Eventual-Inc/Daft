mod decode;

use decode::DecodeEvaluator;
use serde::{Deserialize, Serialize};

use crate::{datatypes::ImageFormat, dsl::Expr};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ImageExpr {
    Decode(ImageFormat),
}

impl ImageExpr {
    #[inline]
    pub fn get_evaluator(&self) -> Box<dyn FunctionEvaluator> {
        use ImageExpr::*;

        match self {
            Decode(image_format) => Box::new(DecodeEvaluator {
                image_format: *image_format,
            }),
        }
    }
}

pub fn decode(input: &Expr, image_format: &ImageFormat) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Decode(*image_format)),
        inputs: vec![input.clone()],
    }
}
