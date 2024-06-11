use crate::ExprRef;
use daft_core::{
    datatypes::{DataType, Field, TimeUnit},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct ToDatetimeEvaluator {}

impl FunctionEvaluator for ToDatetimeEvaluator {
    fn fn_name(&self) -> &'static str {
        "todatetime"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, format] => match (data.to_field(schema), format.to_field(schema)) {
                (Ok(data_field), Ok(format_field)) => {
                    match (&data_field.dtype, &format_field.dtype) {
                        (DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::Timestamp(TimeUnit::Microseconds, None)))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to todatetime to be utf8 and utf8, but received {data_field} and {format_field}",
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
            [data, format] => data.utf8_to_datetime(format),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
