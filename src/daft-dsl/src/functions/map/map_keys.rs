use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use super::super::FunctionEvaluator;
use crate::{ExprRef, functions::FunctionExpr};

pub(super) struct MapKeysEvaluator {}

impl FunctionEvaluator for MapKeysEvaluator {
    fn fn_name(&self) -> &'static str {
        "map_keys"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        let [input] = inputs else {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            )));
        };

        let input_field = input.to_field(schema)?;

        let DataType::Map { key, .. } = input_field.dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected input to be a map, got {}",
                input_field.dtype
            )));
        };

        let field = Field::new("keys", DataType::List(key));

        Ok(field)
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        let [input] = inputs else {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            )));
        };

        input.map_keys()
    }
}
