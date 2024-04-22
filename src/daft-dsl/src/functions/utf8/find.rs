use crate::ExprRef;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct FindEvaluator {}

impl FunctionEvaluator for FindEvaluator {
    fn fn_name(&self) -> &'static str {
        "find"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, substr] => match (data.to_field(schema), substr.to_field(schema)) {
                (Ok(data_field), Ok(substr_field)) => {
                    match (&data_field.dtype, &substr_field.dtype) {
                        (DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::Int64))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to find to be utf8 and utf8, but received {data_field} and {substr_field}",
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
            [data, substr] => data.utf8_find(substr),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
