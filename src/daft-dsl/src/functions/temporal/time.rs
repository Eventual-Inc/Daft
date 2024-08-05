use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{DataType, Field, TimeUnit},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use crate::ExprRef;

use super::super::FunctionEvaluator;

pub(super) struct TimeEvaluator {}

impl FunctionEvaluator for TimeEvaluator {
    fn fn_name(&self) -> &'static str {
        "time"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input] => match input.to_field(schema) {
                Ok(field) => match field.dtype {
                    DataType::Time(_) => Ok(field),
                    DataType::Timestamp(tu, _) => {
                        let tu = match tu {
                            TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                            _ => TimeUnit::Microseconds,
                        };
                        Ok(Field::new(field.name, DataType::Time(tu)))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to time to be time or timestamp, got {}",
                        field.dtype
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input] => input.dt_time(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
