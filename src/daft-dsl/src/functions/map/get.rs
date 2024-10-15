use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use super::super::FunctionEvaluator;
use crate::{functions::FunctionExpr, ExprRef};

pub(super) struct GetEvaluator {}

impl FunctionEvaluator for GetEvaluator {
    fn fn_name(&self) -> &'static str {
        "map_get"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        let [input, key] = inputs else {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            )));
        };

        let input_field = input.to_field(schema)?;
        let _ = key.to_field(schema)?;

        let DataType::Map { value, .. } = input_field.dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected input to be a map, got {}",
                input_field.dtype
            )));
        };

        let field = Field::new("value", *value);

        Ok(field)
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        let [input, key] = inputs else {
            return Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            )));
        };

        input.map_get(key)
    }
}
