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
        match inputs {
            // what is input and what is key
            // input is a map field
            [input, key] => match (input.to_field(schema), key.to_field(schema)) {
                (Ok(input_field), Ok(_)) => match input_field.dtype {
                    DataType::Map { value, .. } => {
                        // todo: perhaps better naming
                        Ok(Field::new("value", *value))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a map, got {}",
                        input_field.dtype
                    ))),
                },
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input, key] => input.map_get(key),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
