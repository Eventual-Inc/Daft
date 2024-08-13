use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use crate::ExprRef;

use crate::functions::FunctionExpr;

pub(super) struct SqrtEvaluator {}

impl FunctionEvaluator for SqrtEvaluator {
    fn fn_name(&self) -> &'static str {
        "sqrt"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [first] => {
                let field = first.to_field(schema)?;
                let dtype = field.dtype.to_floating_representation()?;
                Ok(Field::new(field.name, dtype))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [first] => first.cbrt(),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
