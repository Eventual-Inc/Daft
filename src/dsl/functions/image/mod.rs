mod decode;

use decode::DecodeEvaluator;
use serde::{Deserialize, Serialize};

use crate::{datatypes::DataType, dsl::Expr};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ImageExpr {
    Decode(DataType),
}

impl ImageExpr {
    #[inline]
    pub fn get_evaluator(&self) -> Box<dyn FunctionEvaluator> {
        use ImageExpr::*;

        match self {
            Decode(dtype) => Box::new(DecodeEvaluator {
                dtype: dtype.clone(),
            }),
        }
    }
}

pub fn decode(input: &Expr, dtype: &DataType) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Decode(dtype.clone())),
        inputs: vec![input.clone()],
    }
}
