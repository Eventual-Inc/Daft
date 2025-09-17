use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::*,
    lit::{FromLiteral, Literal},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Single struct for all image hash functions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageHash;

/// Enum to specify which hash algorithm to use
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ImageHashAlgorithm {
    Average,
    Perceptual,
    Difference,
    Wavelet,
    CropResistant,
}

impl std::fmt::Display for ImageHashAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Average => "average",
            Self::Perceptual => "perceptual",
            Self::Difference => "difference",
            Self::Wavelet => "wavelet",
            Self::CropResistant => "crop_resistant",
        })
    }
}

impl From<ImageHashAlgorithm> for Literal {
    fn from(value: ImageHashAlgorithm) -> Self {
        Self::Utf8(value.to_string())
    }
}

impl FromLiteral for ImageHashAlgorithm {
    fn try_from_literal(lit: &Literal) -> DaftResult<Self> {
        if let Literal::Utf8(s) = lit {
            s.parse()
        } else {
            Err(DaftError::ValueError(format!(
                "Expected a string literal, got {:?}",
                lit
            )))
        }
    }
}

impl FromStr for ImageHashAlgorithm {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("average") {
            Ok(Self::Average)
        } else if s.eq_ignore_ascii_case("perceptual") {
            Ok(Self::Perceptual)
        } else if s.eq_ignore_ascii_case("difference") {
            Ok(Self::Difference)
        } else if s.eq_ignore_ascii_case("wavelet") {
            Ok(Self::Wavelet)
        } else if s.eq_ignore_ascii_case("crop_resistant") {
            Ok(Self::CropResistant)
        } else {
            Err(DaftError::ValueError(format!(
                "unsupported hash algorithm: {}. Supported algorithms are: average, perceptual, difference, wavelet, crop_resistant",
                s
            )))
        }
    }
}

/// Function arguments with algorithm parameter
#[derive(FunctionArgs)]
pub struct ImageHashArgs<T> {
    input: T,
    algorithm: ImageHashAlgorithm,
}

/// Single ScalarUDF implementation that handles all hash algorithms
#[typetag::serde]
impl ScalarUDF for ImageHash {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let ImageHashArgs { input, algorithm } = inputs.try_into()?;
        
        match algorithm {
            ImageHashAlgorithm::Average => crate::series::average_hash(&input),
            ImageHashAlgorithm::Perceptual => crate::series::perceptual_hash(&input),
            ImageHashAlgorithm::Difference => crate::series::difference_hash(&input),
            ImageHashAlgorithm::Wavelet => crate::series::wavelet_hash(&input),
            ImageHashAlgorithm::CropResistant => crate::series::crop_resistant_hash(&input),
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
        let ImageHashArgs { input, algorithm: _ } = inputs.try_into()?;
        let field = input.to_field(schema)?;

        match field.dtype {
            DataType::Image(_) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::Utf8))
            }
            _ => Err(DaftError::TypeError(format!(
                "Image hash can only be computed for Image types, got {}",
                field.dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes the hash of an image using the specified algorithm. Returns a 64-character binary string."
    }
}