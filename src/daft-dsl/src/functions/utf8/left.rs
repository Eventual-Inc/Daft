use crate::Expr;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct LeftEvaluator {}

impl FunctionEvaluator for LeftEvaluator {
    fn fn_name(&self) -> &'static str {
        "left"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [data, pattern] => match (data.to_field(schema), pattern.to_field(schema)) {
                (Ok(data_field), Ok(pattern_field)) => {
                    match (&data_field.dtype, &pattern_field.dtype) {
                        (DataType::Utf8, DataType::UInt32) => {
                            Ok(Field::new(data_field.name, DataType::Utf8))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to left to be utf8 and uint32, but received {data_field} and {pattern_field}",
                        ))),
                    }
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [data, pattern] => data.utf8_left(pattern),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
