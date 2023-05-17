use crate::{
    datatypes::{DataType, Field},
    dsl::Expr,
    error::{DaftError, DaftResult},
    schema::Schema,
    series::Series,
};

use super::super::FunctionEvaluator;

pub(super) struct DayOfWeekEvaluator {}

impl FunctionEvaluator for DayOfWeekEvaluator {
    fn fn_name(&self) -> &'static str {
        "day_of_week"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => match input.to_field(schema) {
                Ok(field) if field.dtype.is_temporal() => {
                    Ok(Field::new(field.name, DataType::UInt32))
                }
                Ok(field) => Err(DaftError::TypeError(format!(
                    "Expected input to day to be temporal, got {}",
                    field.dtype
                ))),
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => input.dt_day_of_week(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
