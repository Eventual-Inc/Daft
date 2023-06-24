use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::Expr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct EndswithEvaluator {}

impl FunctionEvaluator for EndswithEvaluator {
    fn fn_name(&self) -> &'static str {
        "endswith"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [data, pattern] => match (data.to_field(schema), pattern.to_field(schema)) {
                (Ok(data_field), Ok(pattern_field)) => {
                    match (&data_field.dtype, &pattern_field.dtype) {
                        (DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::Boolean))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to endswith to be utf8, but received {data_field} and {pattern_field}",
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
            [data, pattern] => data.utf8_endswith(pattern),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
