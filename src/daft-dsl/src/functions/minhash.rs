use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, DataType, Series};
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::{FunctionEvaluator, FunctionExpr};

pub(super) struct MinHashEvaluator {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MinHashExpr {
    num_hashes: usize,
    ngram_size: usize,
    seed: u32,
}

impl FunctionEvaluator for MinHashEvaluator {
    fn fn_name(&self) -> &'static str {
        "minhash"
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        let num_hashes = match expr {
            FunctionExpr::MinHash(MinHashExpr { num_hashes, .. }) => num_hashes,
            _ => panic!("Expected MinHash Expr, got {expr}"),
        };

        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(
                        data_field.name,
                        DataType::FixedSizeList(Box::new(DataType::UInt32), *num_hashes),
                    )),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to minhash to be utf8, but received {data_field}",
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
        let (num_hashes, ngram_size, seed) = match expr {
            FunctionExpr::MinHash(MinHashExpr {
                num_hashes,
                ngram_size,
                seed,
            }) => (num_hashes, ngram_size, seed),
            _ => panic!("Expected MinHash Expr, got {expr}"),
        };

        match inputs {
            [input] => input.minhash(*num_hashes, *ngram_size, *seed),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn minhash(input: ExprRef, num_hashes: usize, ngram_size: usize, seed: u32) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::MinHash(MinHashExpr {
            num_hashes,
            ngram_size,
            seed,
        }),
        inputs: vec![input],
    }
    .into()
}
