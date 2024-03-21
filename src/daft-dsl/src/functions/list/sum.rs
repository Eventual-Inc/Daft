use crate::Expr;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct SumEvaluator {}

impl FunctionEvaluator for SumEvaluator {
    fn fn_name(&self) -> &'static str {
        "sum"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input] => Ok(input.to_field(schema)?.to_exploded_field()?.to_sum()?),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input] => Ok(input.list_sum()?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
