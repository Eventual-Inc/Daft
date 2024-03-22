use crate::Expr;
use daft_core::{datatypes::Field, schema::Schema, series::Series, IntoSeries};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct MeanEvaluator {}

impl FunctionEvaluator for MeanEvaluator {
    fn fn_name(&self) -> &'static str {
        "mean"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input] => Ok(input.to_field(schema)?.to_exploded_field()?.to_mean()?),
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
