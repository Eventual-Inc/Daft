use crate::series::Series;
use common_error::{DaftError, DaftResult};

use crate::datatypes::*;
use crate::series::array_impl::IntoSeries;

impl Series {
    pub fn utf8_endswith(&self, pattern: &Series) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.endswith(pattern.utf8()?)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Endswith not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_startswith(&self, pattern: &Series) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.startswith(pattern.utf8()?)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Startswith not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_contains(&self, pattern: &Series) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.contains(pattern.utf8()?)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Contains not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_split(&self, pattern: &Series) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.split(pattern.utf8()?)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Split not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_length(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.length()?.into_series()),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Length not implemented for type {dt}"
            ))),
        }
    }
}
