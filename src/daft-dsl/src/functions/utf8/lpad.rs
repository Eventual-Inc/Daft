use crate::ExprRef;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct LpadEvaluator {}

impl FunctionEvaluator for LpadEvaluator {
    fn fn_name(&self) -> &'static str {
        "lpad"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, length, pad] => {
                let data = data.to_field(schema)?;
                let length = length.to_field(schema)?;
                let pad = pad.to_field(schema)?;
                if data.dtype == DataType::Utf8
                    && length.dtype.is_integer()
                    && pad.dtype == DataType::Utf8
                {
                    Ok(Field::new(data.name, DataType::Utf8))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expects inputs to lpad to be utf8, integer and utf8, but received {}, {}, and {}", data.dtype, length.dtype, pad.dtype
                    )))
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data, length, pad] => data.utf8_lpad(length, pad),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
