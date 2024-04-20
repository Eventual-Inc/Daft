use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::ExprRef;

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, TemporalExpr};

pub(super) struct TruncateEvaluator {}

impl FunctionEvaluator for TruncateEvaluator {
    fn fn_name(&self) -> &'static str {
        "truncate"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input, start_time] => match (input.to_field(schema), start_time.to_field(schema)) {
                (Ok(input_field), Ok(start_time_field))
                    if input_field.dtype.is_temporal()
                        && (start_time_field.dtype.is_temporal()
                            || start_time_field.dtype.is_null()) =>
                {
                    Ok(Field::new(input_field.name, input_field.dtype))
                }
                (Ok(input_field), Ok(start_time_field)) => Err(DaftError::TypeError(format!(
                    "Expected temporal input args, got {} and {}",
                    input_field.dtype, start_time_field.dtype
                ))),
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], func: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input, start_time] => {
                let freq = match func {
                    FunctionExpr::Temporal(TemporalExpr::Truncate(freq)) => freq,
                    _ => {
                        return Err(DaftError::ValueError(
                            "Expected Temporal function".to_string(),
                        ))
                    }
                };
                input.dt_truncate(freq, start_time)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
