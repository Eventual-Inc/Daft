use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use super::super::FunctionEvaluator;
use crate::{functions::FunctionExpr, ExprRef};

pub(super) struct ContainsEvaluator {}

impl FunctionEvaluator for ContainsEvaluator {
    fn fn_name(&self) -> &'static str {
        "contains"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, pattern] => match (data.to_field(schema), pattern.to_field(schema)) {
                (Ok(data_field), Ok(pattern_field)) => {
                    match (&data_field.dtype, &pattern_field.dtype) {
                        (DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::Boolean))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to contains to be utf8, but received {data_field} and {pattern_field}",
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

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data, pattern] => data.utf8_contains(pattern),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
