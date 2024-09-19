use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use super::{super::FunctionEvaluator, Utf8Expr};
use crate::{functions::FunctionExpr, ExprRef};

pub(super) struct NormalizeEvaluator {}

impl FunctionEvaluator for NormalizeEvaluator {
    fn fn_name(&self) -> &'static str {
        "normalize"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(data_field.name, DataType::Utf8)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to normalize to be utf8, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data] => {
                let opts = match expr {
                    FunctionExpr::Utf8(Utf8Expr::Normalize(opts)) => opts,
                    _ => panic!("Expected Utf8 Normalize Expr, got {expr}"),
                };
                data.utf8_normalize(*opts)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
