use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::ExprRef;

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct IsNanEvaluator {}

impl FunctionEvaluator for IsNanEvaluator {
    fn fn_name(&self) -> &'static str {
        "is_nan"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    // DataType::Float16 |
                    DataType::Float32 | DataType::Float64 => {
                        Ok(Field::new(data_field.name, DataType::Boolean))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to is_nan to be float, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data] => data.is_nan(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
