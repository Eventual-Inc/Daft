use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::DataType, datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use crate::functions::FunctionExpr;
use crate::ExprRef;

pub(super) struct PercentileEvaluator {}

impl FunctionEvaluator for PercentileEvaluator {
    fn fn_name(&self) -> &'static str {
        "get"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input, q] => {
                let input_field = input.to_field(schema)?;
                let q_field = q.to_field(schema)?;
                match q_field.dtype {
                    DataType::Float64 => (),
                    DataType::List(child_dtype) => {
                        if *child_dtype != DataType::Float64 {
                            return Err(DaftError::TypeError(format!(
                                "Expected sketch_percentile q to be of type {}, received: {}",
                                DataType::List(Box::new(DataType::Float64)),
                                DataType::List(child_dtype),
                            )));
                        }
                    }
                    #[cfg(feature = "python")]
                    DataType::Python => (),
                    _ => {
                        return Err(DaftError::TypeError(format!(
                            "Expected sketch_percentile q to be of type {}, received: {}",
                            DataType::Float64,
                            q_field.dtype
                        )))
                    }
                }

                match input_field.dtype {
                    DataType::Struct(_) => Ok(Field::new(
                        input_field.name,
                        DataType::List(Box::new(DataType::Float64)),
                    )),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a struct type, received: {}",
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

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input, q] => input.sketch_percentile(q),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
