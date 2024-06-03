use crate::ExprRef;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct SubstrEvaluator {}

impl FunctionEvaluator for SubstrEvaluator {
    fn fn_name(&self) -> &'static str {
        "substr"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, start, length] => {
                let data = data.to_field(schema)?;
                let start = start.to_field(schema)?;
                let length = length.to_field(schema)?;

                if data.dtype == DataType::Utf8
                    && start.dtype.is_integer()
                    && (length.dtype.is_integer() || length.dtype.is_null())
                {
                    Ok(Field::new(data.name, DataType::Utf8))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expects inputs to substr to be utf8, integer and integer or null but received {}, {} and {}",
                        data.dtype, start.dtype, length.dtype
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
            [data, start, length] => data.utf8_substr(start, length),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
