use crate::{
    datatypes::{DataType, Field},
    dsl::Expr,
    error::{DaftError, DaftResult},
    schema::Schema,
    series::Series,
};

use super::super::FunctionEvaluator;

pub(super) struct IsNanEvaluator {}

impl FunctionEvaluator for IsNanEvaluator {
    fn fn_name(&self) -> &'static str {
        "is_nan"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Float16 | DataType::Float32 | DataType::Float64 => {
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

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data] => data.is_nan(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
