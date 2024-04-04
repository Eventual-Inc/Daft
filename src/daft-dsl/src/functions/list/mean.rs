use crate::Expr;
use daft_core::{
    datatypes::{try_mean_supertype, Field},
    schema::Schema,
    series::Series,
    IntoSeries,
};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct MeanEvaluator {}

impl FunctionEvaluator for MeanEvaluator {
    fn fn_name(&self) -> &'static str {
        "mean"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    try_mean_supertype(&field.dtype)?,
                ))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input] => Ok(input.list_mean()?.into_series()),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
