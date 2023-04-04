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
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let field = inputs.first().unwrap().to_field(schema)?;
        if !field.dtype.is_temporal() {
            return Err(DaftError::TypeError(format!(
                "Expected input to day_of_week to be temporal, got {}",
                field.dtype
            )));
        }
        Ok(Field {
            name: field.name,
            dtype: DataType::UInt32,
        })
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        if inputs.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        inputs.first().unwrap().dt_day_of_week()
    }
}
