use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use crate::{
    functions::{FunctionEvaluator, FunctionExpr},
    ExprRef,
};

pub(super) struct ExpEvaluator {}

impl FunctionEvaluator for ExpEvaluator {
    fn fn_name(&self) -> &'static str {
        "exp"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };
        let field = inputs.first().unwrap().to_field(schema)?;
        let dtype = match field.dtype {
            DataType::Float32 => DataType::Float32,
            dt if dt.is_numeric() => DataType::Float64,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to compute exp to be numeric, got {}",
                    field.dtype
                )))
            }
        };
        Ok(Field::new(field.name, dtype))
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        inputs.first().unwrap().exp()
    }
}
