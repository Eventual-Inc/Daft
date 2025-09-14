use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageAverageHash;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImagePerceptualHash;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageDifferenceHash;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageWaveletHash;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageCropResistantHash;

#[typetag::serde]
impl ScalarUDF for ImageAverageHash {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        crate::series::average_hash(&input)
    }

    fn name(&self) -> &'static str {
        "image_average_hash"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required((0, "input"))?;
        let field = input.to_field(schema)?;

        match field.dtype {
            DataType::Image(_) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::Utf8))
            }
            _ => Err(DaftError::TypeError(format!(
                "Average hash can only be applied to ImageArrays, got {}",
                field.dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes the average hash of an image for deduplication. Returns an 8x8 binary hash as a 64-character string."
    }
}

#[typetag::serde]
impl ScalarUDF for ImagePerceptualHash {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        crate::series::perceptual_hash(&input)
    }

    fn name(&self) -> &'static str {
        "image_perceptual_hash"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required((0, "input"))?;
        let field = input.to_field(schema)?;

        match field.dtype {
            DataType::Image(_) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::Utf8))
            }
            _ => Err(DaftError::TypeError(format!(
                "Perceptual hash can only be applied to ImageArrays, got {}",
                field.dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes the perceptual hash (pHash) of an image using DCT. Returns an 8x8 binary hash as a 64-character string."
    }
}

#[typetag::serde]
impl ScalarUDF for ImageDifferenceHash {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        crate::series::difference_hash(&input)
    }

    fn name(&self) -> &'static str {
        "image_difference_hash"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required((0, "input"))?;
        let field = input.to_field(schema)?;

        match field.dtype {
            DataType::Image(_) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::Utf8))
            }
            _ => Err(DaftError::TypeError(format!(
                "Difference hash can only be applied to ImageArrays, got {}",
                field.dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes the difference hash (dHash) of an image by comparing adjacent pixels. Returns an 8x8 binary hash as a 64-character string."
    }
}

#[typetag::serde]
impl ScalarUDF for ImageWaveletHash {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        crate::series::wavelet_hash(&input)
    }

    fn name(&self) -> &'static str {
        "image_wavelet_hash"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required((0, "input"))?;
        let field = input.to_field(schema)?;

        match field.dtype {
            DataType::Image(_) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::Utf8))
            }
            _ => Err(DaftError::TypeError(format!(
                "Wavelet hash can only be applied to ImageArrays, got {}",
                field.dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes the wavelet hash (wHash) of an image using Haar wavelet transform. Returns an 8x8 binary hash as a 64-character string."
    }
}

#[typetag::serde]
impl ScalarUDF for ImageCropResistantHash {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
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
        let input = inputs.required((0, "input"))?;
        let field = input.to_field(schema)?;

        match field.dtype {
            DataType::Image(_) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::Utf8))
            }
            _ => Err(DaftError::TypeError(format!(
                "Crop-resistant hash can only be applied to ImageArrays, got {}",
                field.dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes the crop-resistant hash (cHash) of an image that is robust to cropping operations. Returns a 64-character binary string."
    }
}
