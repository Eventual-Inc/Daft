use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::*,
    series::{array_impl::IntoSeries, Series},
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

    pub fn utf8_upper(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.upper()?.into_series()))
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
