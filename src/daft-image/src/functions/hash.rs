use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

// ============== Unified Image Hash (ahash/dhash/phash/whash) ==============

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
            "ahash" => crate::series::average_hash(&input),
            "dhash" => crate::series::difference_hash(&input),
            "phash" => crate::series::perceptual_hash(&input),
            "whash" => crate::series::wavelet_hash(&input),
            _ => Err(DaftError::ValueError(format!(
                "Unknown hash algorithm: {algorithm}. Must be one of: ahash, dhash, phash, whash"
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
        let ImageHashArgs {
            input,
            algorithm: _,
        } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        match field.dtype {
            DataType::Image(..) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::FixedSizeBinary(8)))
            }
            _ => Err(DaftError::TypeError(format!(
                "image_hash can only hash ImageArrays and FixedShapeImageArrays, got {field}"
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes a perceptual hash of an image (ahash/dhash/phash/whash)."
    }
}

// ============== Crop-Resistant Hash ==============

#[derive(FunctionArgs)]
struct ImageCropResistantHashArgs<T> {
    input: T,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageCropResistantHash;

#[typetag::serde]
impl ScalarUDF for ImageCropResistantHash {
    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ImageCropResistantHashArgs { input } = inputs.try_into()?;
        crate::series::crop_resistant_hash(&input)
    }

    fn name(&self) -> &'static str {
        "image_crop_resistant_hash"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ImageCropResistantHashArgs { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        match field.dtype {
            DataType::Image(..) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::Binary))
            }
            _ => Err(DaftError::TypeError(format!(
                "ImageCropResistantHash can only hash ImageArrays and FixedShapeImageArrays, got {field}"
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes a crop-resistant hash of an image for deduplication."
    }
}
