use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::*,
    series::{array_impl::IntoSeries, Series},
    with_match_integer_daft_types,
};

impl Series {
    pub fn with_utf8_array(&self, f: impl Fn(&Utf8Array) -> DaftResult<Self>) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Utf8 => f(self.utf8()?),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Operation not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_split(&self, pattern: &Self, regex: bool) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.split(pattern_arr, regex)?.into_series()))
        })
    }

    pub fn utf8_upper(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.upper()?.into_series()))
    }

    pub fn utf8_rstrip(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.rstrip()?.into_series()))
    }

    pub fn utf8_right(&self, nchars: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            if nchars.data_type().is_integer() {
                with_match_integer_daft_types!(nchars.data_type(), |$T| {
                    Ok(arr.right(nchars.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
                })
            } else if nchars.data_type().is_null() {
                Ok(self.clone())
            } else {
                Err(DaftError::TypeError(format!(
                    "Right not implemented for nchar type {}",
                    nchars.data_type()
                )))
            }
        })
    }

    pub fn utf8_substr(&self, start: &Self, length: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            if start.data_type().is_integer() {
                with_match_integer_daft_types!(start.data_type(), |$T| {
                    if length.data_type().is_integer() {
                        with_match_integer_daft_types!(length.data_type(), |$U| {
                            Ok(arr.substr(start.downcast::<<$T as DaftDataType>::ArrayType>()?, Some(length.downcast::<<$U as DaftDataType>::ArrayType>()?))?.into_series())
                        })
                    } else if length.data_type().is_null() {
                        Ok(arr.substr(start.downcast::<<$T as DaftDataType>::ArrayType>()?, None::<&DataArray<Int8Type>>)?.into_series())
                    } else {
                        Err(DaftError::TypeError(format!(
                            "Substr not implemented for length type {}",
                            length.data_type()
                        )))
                    }
            })
            } else if start.data_type().is_null() {
                Ok(self.clone())
            } else {
                Err(DaftError::TypeError(format!(
                    "Substr not implemented for start type {}",
                    start.data_type()
                )))
            }
        })
    }

    pub fn utf8_to_date(&self, format: &str) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.to_date(format)?.into_series()))
    }

    pub fn utf8_to_datetime(&self, format: &str, timezone: Option<&str>) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.to_datetime(format, timezone)?.into_series()))
    }

    pub fn utf8_count_matches(
        &self,
        patterns: &Self,
        whole_word: bool,
        case_sensitive: bool,
    ) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            patterns.with_utf8_array(|pattern_arr| {
                Ok(arr
                    .count_matches(pattern_arr, whole_word, case_sensitive)?
                    .into_series())
            })
        })
    }
}
