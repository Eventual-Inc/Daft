use crate::{
    datatypes::{DataType, Field},
    dsl::Expr,
    error::{DaftError, DaftResult},
    schema::Schema,
    series::Series,
};

use super::super::FunctionEvaluator;

pub struct DecodeEvaluator {}

impl FunctionEvaluator for DecodeEvaluator {
    fn fn_name(&self) -> &'static str {
        "decode"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                if !matches!(field.dtype, DataType::Binary) {
                    return Err(DaftError::TypeError(format!(
                        "ImageDecode can only decode BinaryArrays, got {}",
                        field
                    )));
                }
                Ok(Field::new(
                    field.name,
                    DataType::Image(Box::new(DataType::UInt8), None),
                ))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input] => input.image_decode(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
