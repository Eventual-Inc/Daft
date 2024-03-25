use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::DataType, datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use crate::Expr;

pub(super) struct QuantileEvaluator {}

impl FunctionEvaluator for QuantileEvaluator {
    fn fn_name(&self) -> &'static str {
        "get"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input, q] => {
                let input_field = input.to_field(schema)?;
                let q_field = q.to_field(schema)?;
                if q_field.dtype != DataType::Float64 {
                    return Err(DaftError::TypeError(format!(
                        "Expected approx_quantile q to be of type {}, received: {}",
                        DataType::Float64,
                        q_field.dtype
                    )));
                }

                match input_field.dtype {
                    DataType::Binary => Ok(Field::new(input_field.name, DataType::Float64)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a binary type, received: {}",
                        input_field.dtype
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input, q] => input.approx_quantile(q),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
