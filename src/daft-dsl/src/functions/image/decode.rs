use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::{functions::FunctionExpr, ExprRef};

use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, ImageExpr};

pub struct DecodeEvaluator {}

impl FunctionEvaluator for DecodeEvaluator {
    fn fn_name(&self) -> &'static str {
        "decode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                if !matches!(field.dtype, DataType::Binary) {
                    return Err(DaftError::TypeError(format!(
                        "ImageDecode can only decode BinaryArrays, got {}",
                        field
                    )));
                }
                Ok(Field::new(field.name, DataType::Image(None)))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        let raise_error_on_failure = match expr {
            FunctionExpr::Image(ImageExpr::Decode {
                raise_error_on_failure,
            }) => raise_error_on_failure,
            _ => panic!("DecodeEvaluator expects an Image::Decode expression"),
        };
        match inputs {
            [input] => input.image_decode(*raise_error_on_failure),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
