use common_error::{DaftError, DaftResult};
use xxhash_rust::xxh32::Xxh32;

use crate::{
    array::ops::DaftMinHash,
    datatypes::DataType,
    series::{IntoSeries, Series},
};

struct Xxh32Wrapper {
    inner: Xxh32,
}

impl std::hash::Hasher for Xxh32Wrapper {
    fn write(&mut self, bytes: &[u8]) {
        self.inner.update(bytes);
    }

    fn finish(&self) -> u64 {
        self.inner.digest() as u64
    }
}

struct Xxh32BuildHasher {
    seed: u32,
}

impl Xxh32BuildHasher {
    pub fn new(seed: u32) -> Self {
        Self { seed }
    }
}

impl std::hash::BuildHasher for Xxh32BuildHasher {
    type Hasher = Xxh32Wrapper;

    fn build_hasher(&self) -> Self::Hasher {
        Xxh32Wrapper {
            inner: Xxh32::new(self.seed),
        }
    }
}

impl Series {
    pub fn minhash(
        &self,
        num_hashes: usize,
        ngram_size: usize,
        seed: u32,
        hasher: &impl std::hash::BuildHasher,
    ) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Utf8 => Ok(self
                .utf8()?
                .minhash(num_hashes, ngram_size, seed, hasher)?
                .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "minhash not implemented for {}",
                dt
            ))),
        }
    }
}
