use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::Expr;

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct MonthEvaluator {}

impl FunctionEvaluator for MonthEvaluator {
    fn fn_name(&self) -> &'static str {
        "month"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input] => match input.to_field(schema) {
                Ok(field) if field.dtype.is_temporal() => {
                    Ok(Field::new(field.name, DataType::UInt32))
                }
                Ok(field) => Err(DaftError::TypeError(format!(
                    "Expected input to month to be temporal, got {}",
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

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input] => input.dt_month(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
