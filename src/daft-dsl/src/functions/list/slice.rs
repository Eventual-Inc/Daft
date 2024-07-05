use crate::ExprRef;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

pub(super) struct SliceEvaluator {}

impl FunctionEvaluator for SliceEvaluator {
    fn fn_name(&self) -> &'static str {
        "slice"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input, start, end] => {
                let input_field = input.to_field(schema)?;
                let start_field = start.to_field(schema)?;
                let end_field = end.to_field(schema)?;

                if !start_field.dtype.is_integer() {
                    return Err(DaftError::TypeError(format!(
                        "Expected start index to be integer, received: {}",
                        start_field.dtype
                    )));
                }

                if !end_field.dtype.is_integer() {
                    return Err(DaftError::TypeError(format!(
                        "Expected end index to be integer, received: {}",
                        end_field.dtype
                    )));
                }
                Ok(input_field.to_exploded_field()?.to_list_field()?)
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input, start, end] => input.list_slice(start, end),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
