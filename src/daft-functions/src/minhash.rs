use std::hash::BuildHasherDefault;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use daft_hash::{HashFunctionKind, MurBuildHasher, Sha1Hasher};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MinHashFunction {
    pub num_hashes: usize,
    pub ngram_size: usize,
    pub seed: u32,
    pub hash_function: HashFunctionKind,
}

#[typetag::serde]
impl ScalarUDF for MinHashFunction {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "minhash"
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        let [input] = inputs else {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };

        match self.hash_function {
            HashFunctionKind::MurmurHash3 => {
                let hasher = MurBuildHasher::new(self.seed);
                input.minhash(self.num_hashes, self.ngram_size, self.seed, &hasher)
            }
            HashFunctionKind::XxHash => {
                let hasher = xxhash_rust::xxh64::Xxh64Builder::new(self.seed as u64);
                input.minhash(self.num_hashes, self.ngram_size, self.seed, &hasher)
            }
            HashFunctionKind::Sha1 => {
                let hasher = BuildHasherDefault::<Sha1Hasher>::default();
                input.minhash(self.num_hashes, self.ngram_size, self.seed, &hasher)
            }
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(field) => match &field.dtype {
                    DataType::Utf8 => Ok(Field::new(
                        field.name,
                        DataType::FixedSizeList(Box::new(DataType::UInt32), self.num_hashes),
                    )),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to minhash to be utf8, but received {field}",
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
}

#[must_use]
pub fn minhash(
    input: ExprRef,
    num_hashes: usize,
    ngram_size: usize,
    seed: u32,
    hash_function: HashFunctionKind,
) -> ExprRef {
    ScalarFunction::new(
        MinHashFunction {
            num_hashes,
            ngram_size,
            seed,
            hash_function,
        },
        vec![input],
    )
    .into()
}
