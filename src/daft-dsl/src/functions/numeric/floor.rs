use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use crate::functions::FunctionExpr;
use crate::ExprRef;

pub(super) struct FloorEvaluator {}

impl FunctionEvaluator for FloorEvaluator {
    fn fn_name(&self) -> &'static str {
        "floor"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let field = inputs.first().unwrap().to_field(schema)?;
        if !field.dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "Expected input to floor to be numeric, got {}",
                field.dtype
            )));
        }
        Ok(field)
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        inputs.first().unwrap().floor()
    }
}
