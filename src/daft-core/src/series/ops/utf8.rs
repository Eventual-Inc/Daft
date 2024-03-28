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

    pub fn utf8_match(&self, pattern: &Series) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.match_(pattern.utf8()?)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Match not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_split(&self, pattern: &Series, regex: bool) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.split(pattern.utf8()?, regex)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Split not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_extract(&self, pattern: &Series, index: usize) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.extract(pattern.utf8()?, index)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Extract not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_extract_all(&self, pattern: &Series, index: usize) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self
                .utf8()?
                .extract_all(pattern.utf8()?, index)?
                .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "ExtractAll not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_replace(
        &self,
        pattern: &Series,
        replacement: &Series,
        regex: bool,
        replace_all: bool,
    ) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self
                .utf8()?
                .replace(pattern.utf8()?, replacement.utf8()?, regex, replace_all)?
                .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Replace not implemented for type {dt}"
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

    pub fn utf8_lower(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.lower()?.into_series()),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Lower not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_upper(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.upper()?.into_series()),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Upper not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_lstrip(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.lstrip()?.into_series()),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Lstrip not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_rstrip(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.rstrip()?.into_series()),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Rstrip not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_reverse(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.reverse()?.into_series()),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Reverse not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_capitalize(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.capitalize()?.into_series()),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Capitalize not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_left(&self, nchars: &Series) -> DaftResult<Series> {
        match (self.data_type(), nchars.data_type()) {
            (DataType::Utf8, DataType::UInt32) => {
                Ok(self.utf8()?.left(nchars.u32()?)?.into_series())
            }
            (DataType::Utf8, DataType::Int32) => {
                Ok(self.utf8()?.left(nchars.i32()?)?.into_series())
            }
            (DataType::Utf8, DataType::UInt64) => {
                Ok(self.utf8()?.left(nchars.u64()?)?.into_series())
            }
            (DataType::Utf8, DataType::Int64) => {
                Ok(self.utf8()?.left(nchars.i64()?)?.into_series())
            }
            (DataType::Utf8, DataType::Int8) => Ok(self.utf8()?.left(nchars.i8()?)?.into_series()),
            (DataType::Utf8, DataType::UInt8) => Ok(self.utf8()?.left(nchars.u8()?)?.into_series()),
            (DataType::Utf8, DataType::Int16) => {
                Ok(self.utf8()?.left(nchars.i16()?)?.into_series())
            }
            (DataType::Utf8, DataType::UInt16) => {
                Ok(self.utf8()?.left(nchars.u16()?)?.into_series())
            }
            (DataType::Utf8, DataType::Int128) => {
                Ok(self.utf8()?.left(nchars.i128()?)?.into_series())
            }
            (DataType::Null, dt) if dt.is_integer() => Ok(self.clone()),
            (DataType::Utf8, dt) => Err(DaftError::TypeError(format!(
                "Left not implemented for nchar type {dt}"
            ))),
            (dt, _) => Err(DaftError::TypeError(format!(
                "Left not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_right(&self, nchars: &Series) -> DaftResult<Series> {
        match (self.data_type(), nchars.data_type()) {
            (DataType::Utf8, DataType::UInt32) => {
                Ok(self.utf8()?.right(nchars.u32()?)?.into_series())
            }
            (DataType::Utf8, DataType::Int32) => {
                Ok(self.utf8()?.right(nchars.i32()?)?.into_series())
            }
            (DataType::Utf8, DataType::UInt64) => {
                Ok(self.utf8()?.right(nchars.u64()?)?.into_series())
            }
            (DataType::Utf8, DataType::Int64) => {
                Ok(self.utf8()?.right(nchars.i64()?)?.into_series())
            }
            (DataType::Utf8, DataType::Int8) => Ok(self.utf8()?.right(nchars.i8()?)?.into_series()),
            (DataType::Utf8, DataType::UInt8) => {
                Ok(self.utf8()?.right(nchars.u8()?)?.into_series())
            }
            (DataType::Utf8, DataType::Int16) => {
                Ok(self.utf8()?.right(nchars.i16()?)?.into_series())
            }
            (DataType::Utf8, DataType::UInt16) => {
                Ok(self.utf8()?.right(nchars.u16()?)?.into_series())
            }
            (DataType::Utf8, DataType::Int128) => {
                Ok(self.utf8()?.right(nchars.i128()?)?.into_series())
            }
            (DataType::Null, dt) if dt.is_integer() => Ok(self.clone()),
            (DataType::Utf8, dt) => Err(DaftError::TypeError(format!(
                "Right not implemented for nchar type {dt}"
            ))),
            (dt, _) => Err(DaftError::TypeError(format!(
                "Right not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_find(&self, substr: &Series) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.find(substr.utf8()?)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Find not implemented for type {dt}"
            ))),
        }
    }
}
