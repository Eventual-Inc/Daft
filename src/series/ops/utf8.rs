use crate::{
    error::{DaftError, DaftResult},
    series::Series,
};

use crate::array::BaseArray;
use crate::datatypes::*;

impl Series {
    pub fn utf8_endswith(&self, pattern: &Series) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.endswith(pattern.utf8()?)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Endswith not implemented for type {dt}"
            ))),
        }
    }
}
