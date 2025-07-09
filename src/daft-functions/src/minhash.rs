use std::hash::BuildHasherDefault;

use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::functions::prelude::*;
use daft_hash::{HashFunctionKind, MurBuildHasher, Sha1Hasher};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MinHashFunction;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    num_hashes: usize,
    ngram_size: usize,
    #[arg(optional)]
    seed: Option<u32>,
    #[arg(optional)]
    hash_function: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for MinHashFunction {
    fn name(&self) -> &'static str {
        "minhash"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args {
            input,
            num_hashes,
            ngram_size,
            seed,
            hash_function,
        } = inputs.try_into()?;

        let seed = seed.unwrap_or(1);

        let hash_function = hash_function
            .map(|s| s.parse::<HashFunctionKind>())
            .transpose()?
            .unwrap_or_default();

        match hash_function {
            HashFunctionKind::MurmurHash3 => {
                let hasher = MurBuildHasher::new(seed);
                input.minhash(num_hashes, ngram_size, seed, &hasher)
            }
            HashFunctionKind::XxHash => {
                let hasher = xxhash_rust::xxh64::Xxh64Builder::new(seed as u64);
                input.minhash(num_hashes, ngram_size, seed, &hasher)
            }
            HashFunctionKind::Sha1 => {
                let hasher = BuildHasherDefault::<Sha1Hasher>::default();
                input.minhash(num_hashes, ngram_size, seed, &hasher)
            }
        }
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args {
            input, num_hashes, ..
        } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(
            input.dtype.is_string(),
            TypeError: "Expects input to minhash to be utf8, but received {}",
            input.dtype
        );
        Ok(Field::new(
            input.name,
            DataType::FixedSizeList(Box::new(DataType::UInt32), num_hashes),
        ))
    }
}
