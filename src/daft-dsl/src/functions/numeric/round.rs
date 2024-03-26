use common_error::{DaftError, DaftResult};
use daft_core::DataType;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use crate::Expr;

pub(super) struct RoundEvaluator {}

impl FunctionEvaluator for RoundEvaluator {
    fn fn_name(&self) -> &'static str {
        "round"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input, digits] => {
                let input_field = input.to_field(schema)?;
                let digits_field = digits.to_field(schema)?;
                if digits_field.dtype != DataType::Int32 {
                    return Err(DaftError::TypeError(format!(
                        "Expected round digits to be of type {}, received: {}",
                        DataType::Int32,
                        digits_field.dtype
                    )));
                }

                if !input_field.dtype.is_numeric() {
                    return Err(DaftError::TypeError(format!(
                        "Expected input to round to be numeric, got {}",
                        input_field.dtype
                    )));
                }
                Ok(input_field)
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input, digits] => {
                if digits.len() != 1 {
                    return Err(DaftError::ValueError(format!(
                        "Expected digits length to be 1 got {}",
                        digits.len()
                    )));
                }
                if digits.i32().unwrap().get(0).unwrap() < 0 {
                    return Err(DaftError::ValueError(format!(
                        "Expected digits to be greater than or equal to 0, got {}",
                        digits.i32().unwrap().get(0).unwrap()
                    )));
                }
                let digits = digits.i32().unwrap().get(0).unwrap();
                input.round(digits)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
