use std::{
    hash::{BuildHasher, Hasher},
    sync::Arc,
};

use arrow_buffer::NullBufferBuilder;
use daft_common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use daft_hash::{HashFunctionKind, MurBuildHasher, XxHash32BuildHasher};
use serde::{Deserialize, Serialize};

const NUM_BITS: usize = 64;

fn compute_simhash(s: &str, ngram_size: usize, hash_function: HashFunctionKind) -> u64 {
    let mut weights = [0i32; NUM_BITS];

    let chars: Vec<char> = s.chars().collect();
    if chars.len() < ngram_size {
        return 0;
    }

    for window in chars.windows(ngram_size) {
        let ngram: String = window.iter().collect();
        let hash = hash_ngram(&ngram, hash_function);

        for (i, w) in weights.iter_mut().enumerate() {
            if hash & (1u64 << i) != 0 {
                *w += 1;
            } else {
                *w -= 1;
            }
        }
    }

    let mut fingerprint: u64 = 0;
    for (i, &w) in weights.iter().enumerate() {
        if w > 0 {
            fingerprint |= 1u64 << i;
        }
    }
    fingerprint
}

fn hash_ngram(ngram: &str, hash_function: HashFunctionKind) -> u64 {
    match hash_function {
        HashFunctionKind::MurmurHash3 => {
            let bh = MurBuildHasher::new(0);
            let mut h = bh.build_hasher();
            h.write(ngram.as_bytes());
            h.finish()
        }
        HashFunctionKind::XxHash32 => {
            let bh = XxHash32BuildHasher::new(0);
            let mut h = bh.build_hasher();
            h.write(ngram.as_bytes());
            h.finish()
        }
        HashFunctionKind::XxHash64 => {
            let bh = xxhash_rust::xxh64::Xxh64Builder::new(0);
            let mut h = bh.build_hasher();
            h.write(ngram.as_bytes());
            h.finish()
        }
        HashFunctionKind::XxHash3_64 => {
            let bh = xxhash_rust::xxh3::Xxh3Builder::new().with_seed(0);
            let mut h = bh.build_hasher();
            h.write(ngram.as_bytes());
            h.finish()
        }
        HashFunctionKind::Sha1 => {
            let bh = std::hash::BuildHasherDefault::<daft_hash::Sha1Hasher>::default();
            let mut h = bh.build_hasher();
            h.write(ngram.as_bytes());
            h.finish()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SimHashFunction;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    #[arg(optional)]
    ngram_size: Option<usize>,
    #[arg(optional)]
    hash_function: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for SimHashFunction {
    fn name(&self) -> &'static str {
        "simhash"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Args {
            input,
            ngram_size,
            hash_function,
        } = inputs.try_into()?;

        let ngram_size = ngram_size.unwrap_or(3);
        if ngram_size == 0 {
            return Err(DaftError::ValueError(
                "ngram_size must be a positive integer".to_string(),
            ));
        }

        let hash_function = hash_function
            .map(|s| s.parse::<HashFunctionKind>())
            .transpose()?
            .unwrap_or(HashFunctionKind::XxHash3_64);

        input.with_utf8_array(|utf8_arr| {
            let len = utf8_arr.len();
            let mut values = Vec::with_capacity(len);
            let mut validity = NullBufferBuilder::new(len);

            for i in 0..len {
                match utf8_arr.get(i) {
                    Some(s) => {
                        values.push(compute_simhash(s, ngram_size, hash_function));
                        validity.append_non_null();
                    }
                    None => {
                        values.push(0);
                        validity.append_null();
                    }
                }
            }

            let field = Arc::new(Field::new(utf8_arr.name(), DataType::UInt64));
            let result =
                UInt64Array::from_field_and_values(field, values).with_nulls(validity.finish())?;
            Ok(result.into_series())
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, .. } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(
            input.dtype.is_string(),
            TypeError: "Expected input to simhash to be a string type, but received {}",
            input.dtype
        );
        Ok(Field::new(input.name, DataType::UInt64))
    }

    fn docstring(&self) -> &'static str {
        "Compute a SimHash fingerprint (UInt64) from text using character n-grams. \
        Similar texts produce fingerprints with small bitwise Hamming distance."
    }
}

#[must_use]
pub fn simhash(
    input: ExprRef,
    ngram_size: Option<ExprRef>,
    hash_function: Option<ExprRef>,
) -> ExprRef {
    let mut inputs = vec![input];
    if let Some(n) = ngram_size {
        inputs.push(n);
    }
    if let Some(h) = hash_function {
        inputs.push(h);
    }
    ScalarFn::builtin(SimHashFunction, inputs).into()
}
