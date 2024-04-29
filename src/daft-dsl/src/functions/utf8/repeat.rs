use crate::ExprRef;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct RepeatEvaluator {}

impl FunctionEvaluator for RepeatEvaluator {
    fn fn_name(&self) -> &'static str {
        "repeat"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, ntimes] => match (data.to_field(schema), ntimes.to_field(schema)) {
                (Ok(data_field), Ok(ntimes_field)) => {
                    match (&data_field.dtype, &ntimes_field.dtype) {
                        (DataType::Utf8, dt) if dt.is_integer() => {
                            Ok(Field::new(data_field.name, DataType::Utf8))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to repeat to be utf8 and integer, but received {data_field} and {ntimes_field}",
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
            [data, ntimes] => data.utf8_repeat(ntimes),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
