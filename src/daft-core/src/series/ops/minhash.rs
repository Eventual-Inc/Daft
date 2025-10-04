use std::hash::BuildHasherDefault;

use common_error::{DaftError, DaftResult};
use daft_hash::{HashFunctionKind, MurBuildHasher, Sha1Hasher, XxHash32BuildHasher};

use crate::{
    array::ops::DaftMinHash,
    datatypes::DataType,
    series::{IntoSeries, Series},
};

impl Series {
    pub fn minhash(
        &self,
        num_hashes: usize,
        ngram_size: usize,
        seed: u32,
        hash_function: HashFunctionKind,
    ) -> DaftResult<Self> {
        let arr = match self.data_type() {
            DataType::Utf8 => self.utf8()?,
            dt => {
                return Err(DaftError::TypeError(format!(
                    "minhash not implemented for {}",
                    dt
                )));
            }
        };

        let output = match hash_function {
            HashFunctionKind::MurmurHash3 => {
                let hasher = MurBuildHasher::new(seed);
                arr.minhash(num_hashes, ngram_size, seed, &hasher)
            }
            HashFunctionKind::XxHash64 => {
                let hasher = xxhash_rust::xxh64::Xxh64Builder::new(seed as u64);
                arr.minhash(num_hashes, ngram_size, seed, &hasher)
            }
            HashFunctionKind::XxHash32 => {
                let hasher = XxHash32BuildHasher::new(seed);
                arr.minhash(num_hashes, ngram_size, seed, &hasher)
            }
            HashFunctionKind::XxHash3_64 => {
                let hasher = xxhash_rust::xxh3::Xxh3Builder::new().with_seed(seed as u64);
                arr.minhash(num_hashes, ngram_size, seed, &hasher)
            }
            HashFunctionKind::Sha1 => {
                let hasher = BuildHasherDefault::<Sha1Hasher>::default();
                arr.minhash(num_hashes, ngram_size, seed, &hasher)
            }
        }?;

        Ok(output.into_series())
    }
}
