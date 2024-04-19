use crate::Expr;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct RpadEvaluator {}

impl FunctionEvaluator for RpadEvaluator {
    fn fn_name(&self) -> &'static str {
        "rpad"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, length, pad] => match (
                data.to_field(schema),
                length.to_field(schema),
                pad.to_field(schema),
            ) {
                (Ok(data_field), Ok(length_field), Ok(pad_field)) => {
                    match (&data_field.dtype, &length_field.dtype, &pad_field.dtype) {
                        (DataType::Utf8, dt, DataType::Utf8) if dt.is_integer() => {
                            Ok(Field::new(data_field.name, DataType::Utf8))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to rpad to be utf8, integer and utf8, but received {data_field}, {length_field}, and {pad_field}",
                        ))),
                    }
                }
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data, length, pad] => data.utf8_rpad(length, pad),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
