use common_error::DaftError;
use common_error::DaftResult;
use daft_core::{datatypes::Field, schema::Schema, Series};

use crate::{
    functions::{FunctionEvaluator, FunctionExpr},
    ExprRef,
};

pub(super) struct CbrtEvaluator;

impl FunctionEvaluator for CbrtEvaluator {
    fn fn_name(&self) -> &'static str {
        "cbrt"
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
