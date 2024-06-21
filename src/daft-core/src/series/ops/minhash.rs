use common_error::{DaftError, DaftResult};

use crate::{array::ops::DaftMinHash, DataType, IntoSeries, Series};

impl Series {
    pub fn minhash(
        &self,
        num_hashes: usize,
        ngram_size: usize,
        permutations: &[u32],
        hash_seed: Option<u32>,
    ) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self
                .utf8()?
                .minhash(num_hashes, ngram_size, permutations, hash_seed)?
                .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "minhash not implemented for {}",
                dt
            ))),
        }
    }
}
