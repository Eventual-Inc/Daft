use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use crate::ExprRef;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct LengthEvaluator {}

impl FunctionEvaluator for LengthEvaluator {
    fn fn_name(&self) -> &'static str {
        "length"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(data_field.name, DataType::UInt64)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to length to be utf8, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data] => data.utf8_length(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
