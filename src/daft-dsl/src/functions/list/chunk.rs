use crate::ExprRef;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use super::{super::FunctionEvaluator, ListExpr};
use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

pub(super) struct ChunkEvaluator {}

impl FunctionEvaluator for ChunkEvaluator {
    fn fn_name(&self) -> &'static str {
        "chunk"
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        let size = match expr {
            FunctionExpr::List(ListExpr::Chunk(size)) => size,
            _ => panic!("Expected Chunk Expr, got {expr}"),
        };
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;
                Ok(input_field
                    .to_exploded_field()?
                    .to_fixed_size_list_field(*size)?
                    .to_list_field()?)
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        let size = match expr {
            FunctionExpr::List(ListExpr::Chunk(size)) => size,
            _ => panic!("Expected Chunk Expr, got {expr}"),
        };
        match inputs {
            [input] => input.list_chunk(*size),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
