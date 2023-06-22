use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::Expr;

use super::super::FunctionEvaluator;

pub(super) struct AbsEvaluator {}

impl FunctionEvaluator for AbsEvaluator {
    fn fn_name(&self) -> &'static str {
        "abs"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let field = inputs.first().unwrap().to_field(schema)?;
        if !field.dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "Expected input to abs to be numeric, got {}",
                field.dtype
            )));
        }
        Ok(field)
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        if inputs.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        inputs.first().unwrap().abs()
    }
}
