use crate::functions::FunctionExpr;
use crate::ExprRef;
use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;

pub(super) struct ExplodeEvaluator {}

impl FunctionEvaluator for ExplodeEvaluator {
    fn fn_name(&self) -> &'static str {
        "explode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                field.to_exploded_field()
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input] => input.explode(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
