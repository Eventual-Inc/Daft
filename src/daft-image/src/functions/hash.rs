use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

#[derive(FunctionArgs)]
struct ImageHashArgs<T> {
    input: T,
    algorithm: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageHash;

#[typetag::serde]
impl ScalarUDF for ImageHash {
    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ImageHashArgs { input, algorithm } = inputs.try_into()?;
        match algorithm.as_str() {
            "average" => crate::series::average_hash(&input),
            "difference" => crate::series::difference_hash(&input),
            "perceptual" => crate::series::perceptual_hash(&input),
            "wavelet" => crate::series::wavelet_hash(&input),
            "crop_resistant" => crate::series::crop_resistant_hash(&input),
            _ => Err(DaftError::ValueError(format!(
                "Unknown hash algorithm: {algorithm}. Must be one of: average, difference, perceptual, wavelet, crop_resistant"
            ))),
        }
    }

    fn name(&self) -> &'static str {
        "image_hash"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ImageHashArgs { input, algorithm } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        match field.dtype {
            DataType::Image(..) | DataType::FixedShapeImage(..) => match algorithm.as_str() {
                "average" | "difference" | "perceptual" | "wavelet" => {
                    Ok(Field::new(field.name, DataType::UInt64))
                }
                "crop_resistant" => Ok(Field::new(field.name, DataType::Binary)),
                _ => Err(DaftError::ValueError(format!(
                    "Unknown hash algorithm: {algorithm}. Must be one of: average, difference, perceptual, wavelet, crop_resistant"
                ))),
            },
            _ => Err(DaftError::TypeError(format!(
                "image_hash can only hash ImageArrays and FixedShapeImageArrays, got {field}"
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes a perceptual hash of an image (average/difference/perceptual/wavelet/crop_resistant)."
    }
}
