use common_error::{DaftError, DaftResult};

use crate::{
    array::ops::DaftMinHash,
    datatypes::DataType,
    series::{IntoSeries, Series},
};

impl Series {
    pub fn minhash(&self, num_hashes: usize, ngram_size: usize, seed: u32) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Utf8 => Ok(self
                .utf8()?
                .minhash(num_hashes, ngram_size, seed)?
                .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "minhash not implemented for {}",
                dt
            ))),
        }
    }
}
