use common_error::{DaftError, DaftResult};
use daft_core::{
    lit::{FromLiteral, Literal},
    prelude::*,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageHash;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ImageHashAlgorithm {
    Average,
    Perceptual,
    Difference,
    Wavelet,
    CropResistant,
}

impl std::str::FromStr for ImageHashAlgorithm {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "average" => Ok(Self::Average),
            "perceptual" => Ok(Self::Perceptual),
            "difference" => Ok(Self::Difference),
            "wavelet" => Ok(Self::Wavelet),
            "crop_resistant" => Ok(Self::CropResistant),
            _ => Err(DaftError::ValueError(format!(
                "Invalid image hash algorithm: {}. Must be one of: average, perceptual, difference, wavelet, crop_resistant",
                s
            ))),
        }
    }
}

impl FromLiteral for ImageHashAlgorithm {
    fn try_from_literal(lit: &Literal) -> DaftResult<Self> {
        match lit {
            Literal::Utf8(s) => s.parse(),
            _ => Err(DaftError::TypeError(format!(
                "Expected string literal for image hash algorithm, got: {:?}",
                lit
            ))),
        }
    }
}

#[typetag::serde]
impl ScalarUDF for ImageHash {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let algorithm_series = inputs.required((1, "algorithm"))?;
        
        // Extract the algorithm from the series (should be a scalar)
        let algorithm_str = algorithm_series.utf8()?.get(0).ok_or_else(|| {
            DaftError::ValueError("algorithm must be a scalar string".to_string())
        })?;
        
        let algorithm: ImageHashAlgorithm = algorithm_str.parse()?;
        
        // Convert enum to string and call the unified image_hash function
        let algorithm_str = match algorithm {
            ImageHashAlgorithm::Average => "average",
            ImageHashAlgorithm::Perceptual => "perceptual", 
            ImageHashAlgorithm::Difference => "difference",
            ImageHashAlgorithm::Wavelet => "wavelet",
            ImageHashAlgorithm::CropResistant => "crop_resistant",
        };
        
        crate::series::image_hash(input, algorithm_str)
    }

    fn name(&self) -> &'static str {
        "image_hash"
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
                "Image hash can only be applied to ImageArrays, got {}",
                field.dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes the hash of an image using the specified algorithm. Supports average, perceptual, difference, wavelet, and crop_resistant algorithms."
    }
}
