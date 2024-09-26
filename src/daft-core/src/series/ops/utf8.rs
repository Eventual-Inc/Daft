use common_error::{DaftError, DaftResult};

use crate::{
    array::ops::{PadPlacement, Utf8NormalizeOptions},
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

    pub fn utf8_endswith(&self, pattern: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.endswith(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_startswith(&self, pattern: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.startswith(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_contains(&self, pattern: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.contains(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_match(&self, pattern: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.match_(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_split(&self, pattern: &Self, regex: bool) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.split(pattern_arr, regex)?.into_series()))
        })
    }

    pub fn utf8_extract(&self, pattern: &Self, index: usize) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern
                .with_utf8_array(|pattern_arr| Ok(arr.extract(pattern_arr, index)?.into_series()))
        })
    }

    pub fn utf8_extract_all(&self, pattern: &Self, index: usize) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| {
                Ok(arr.extract_all(pattern_arr, index)?.into_series())
            })
        })
    }

    pub fn utf8_replace(
        &self,
        pattern: &Self,
        replacement: &Self,
        regex: bool,
    ) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| {
                replacement.with_utf8_array(|replacement_arr| {
                    Ok(arr
                        .replace(pattern_arr, replacement_arr, regex)?
                        .into_series())
                })
            })
        })
    }

    pub fn utf8_length(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.length()?.into_series()))
    }

    pub fn utf8_length_bytes(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.length_bytes()?.into_series()))
    }

    pub fn utf8_lower(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.lower()?.into_series()))
    }

    pub fn utf8_upper(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.upper()?.into_series()))
    }

    pub fn utf8_lstrip(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.lstrip()?.into_series()))
    }

    pub fn utf8_rstrip(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.rstrip()?.into_series()))
    }

    pub fn utf8_reverse(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.reverse()?.into_series()))
    }

    pub fn utf8_capitalize(&self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.capitalize()?.into_series()))
    }

    pub fn utf8_left(&self, nchars: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            if nchars.data_type().is_integer() {
                with_match_integer_daft_types!(nchars.data_type(), |$T| {
                    Ok(arr.left(nchars.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
                })
            } else if nchars.data_type().is_null() {
                Ok(self.clone())
            } else {
                Err(DaftError::TypeError(format!(
                    "Left not implemented for nchar type {}",
                    nchars.data_type()
                )))
            }
        })
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

    pub fn utf8_find(&self, substr: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            substr.with_utf8_array(|substr_arr| Ok(arr.find(substr_arr)?.into_series()))
        })
    }

    pub fn utf8_lpad(&self, length: &Self, pad: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pad.with_utf8_array(|pad_arr| {
                if length.data_type().is_integer() {
                    with_match_integer_daft_types!(length.data_type(), |$T| {
                        Ok(arr.pad(length.downcast::<<$T as DaftDataType>::ArrayType>()?, pad_arr, PadPlacement::Left)?.into_series())
                    })
                } else if length.data_type().is_null() {
                    Ok(self.clone())
                } else {
                    Err(DaftError::TypeError(format!(
                        "Lpad not implemented for length type {}",
                        length.data_type()
                    )))
                }
            })
        })
    }

    pub fn utf8_rpad(&self, length: &Self, pad: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pad.with_utf8_array(|pad_arr| {
                if length.data_type().is_integer() {
                    with_match_integer_daft_types!(length.data_type(), |$T| {
                        Ok(arr.pad(length.downcast::<<$T as DaftDataType>::ArrayType>()?, pad_arr, PadPlacement::Right)?.into_series())
                    })
                } else if length.data_type().is_null() {
                    Ok(self.clone())
                } else {
                    Err(DaftError::TypeError(format!(
                        "Rpad not implemented for length type {}",
                        length.data_type()
                    )))
                }
            })
        })
    }

    pub fn utf8_repeat(&self, n: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            if n.data_type().is_integer() {
                with_match_integer_daft_types!(n.data_type(), |$T| {
                    Ok(arr.repeat(n.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
                })
            } else if n.data_type().is_null() {
                Ok(self.clone())
            } else {
                Err(DaftError::TypeError(format!(
                    "Repeat not implemented for nchar type {}",
                    n.data_type()
                )))
            }
        })
    }

    pub fn utf8_like(&self, pattern: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.like(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_ilike(&self, pattern: &Self) -> DaftResult<Self> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.ilike(pattern_arr)?.into_series()))
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

    pub fn utf8_normalize(&self, opts: Utf8NormalizeOptions) -> DaftResult<Self> {
        self.with_utf8_array(|arr| Ok(arr.normalize(opts)?.into_series()))
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
