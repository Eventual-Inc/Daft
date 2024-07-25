mod channels;
mod crop;
mod decode;
mod encode;
mod height;
mod mode;
mod resize;
mod width;

use channels::ChannelsEvaluator;
use crop::CropEvaluator;
use decode::DecodeEvaluator;
use encode::EncodeEvaluator;
use height::HeightEvaluator;
use mode::ModeEvaluator;
use resize::ResizeEvaluator;
use serde::{Deserialize, Serialize};
use width::WidthEvaluator;

use daft_core::datatypes::ImageFormat;

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ImageExpr {
    Decode { raise_error_on_failure: bool },
    Encode { image_format: ImageFormat },
    Resize { w: u32, h: u32 },
    Crop(),
    Width(),
    Height(),
    Channels(),
    Mode(),
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
            Width { .. } => &WidthEvaluator {},
            Height { .. } => &HeightEvaluator {},
            Channels { .. } => &ChannelsEvaluator {},
            Mode { .. } => &ModeEvaluator {},
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

pub fn width(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Width()),
        inputs: vec![input],
    }
    .into()
}

pub fn height(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Height()),
        inputs: vec![input],
    }
    .into()
}

pub fn channels(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Channels()),
        inputs: vec![input],
    }
    .into()
}

pub fn mode(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Image(ImageExpr::Mode()),
        inputs: vec![input],
    }
    .into()
}
