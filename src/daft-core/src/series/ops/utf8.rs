use crate::array::ops::PadPlacement;
use crate::series::Series;
use common_error::{DaftError, DaftResult};

use crate::series::array_impl::IntoSeries;
use crate::{datatypes::*, with_match_integer_daft_types};

impl Series {
    fn with_utf8_array(&self, f: impl Fn(&Utf8Array) -> DaftResult<Series>) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => f(self.utf8()?),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Operation not implemented for type {dt}"
            ))),
        }
    }

    pub fn utf8_endswith(&self, pattern: &Series) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.endswith(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_startswith(&self, pattern: &Series) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.startswith(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_contains(&self, pattern: &Series) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.contains(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_match(&self, pattern: &Series) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.match_(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_split(&self, pattern: &Series, regex: bool) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.split(pattern_arr, regex)?.into_series()))
        })
    }

    pub fn utf8_extract(&self, pattern: &Series, index: usize) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            pattern
                .with_utf8_array(|pattern_arr| Ok(arr.extract(pattern_arr, index)?.into_series()))
        })
    }

    pub fn utf8_extract_all(&self, pattern: &Series, index: usize) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| {
                Ok(arr.extract_all(pattern_arr, index)?.into_series())
            })
        })
    }

    pub fn utf8_replace(
        &self,
        pattern: &Series,
        replacement: &Series,
        regex: bool,
    ) -> DaftResult<Series> {
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

    pub fn utf8_length(&self) -> DaftResult<Series> {
        self.with_utf8_array(|arr| Ok(arr.length()?.into_series()))
    }

    pub fn utf8_lower(&self) -> DaftResult<Series> {
        self.with_utf8_array(|arr| Ok(arr.lower()?.into_series()))
    }

    pub fn utf8_upper(&self) -> DaftResult<Series> {
        self.with_utf8_array(|arr| Ok(arr.upper()?.into_series()))
    }

    pub fn utf8_lstrip(&self) -> DaftResult<Series> {
        self.with_utf8_array(|arr| Ok(arr.lstrip()?.into_series()))
    }

    pub fn utf8_rstrip(&self) -> DaftResult<Series> {
        self.with_utf8_array(|arr| Ok(arr.rstrip()?.into_series()))
    }

    pub fn utf8_reverse(&self) -> DaftResult<Series> {
        self.with_utf8_array(|arr| Ok(arr.reverse()?.into_series()))
    }

    pub fn utf8_capitalize(&self) -> DaftResult<Series> {
        self.with_utf8_array(|arr| Ok(arr.capitalize()?.into_series()))
    }

    pub fn utf8_left(&self, nchars: &Series) -> DaftResult<Series> {
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

    pub fn utf8_right(&self, nchars: &Series) -> DaftResult<Series> {
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

    pub fn utf8_find(&self, substr: &Series) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            substr.with_utf8_array(|substr_arr| Ok(arr.find(substr_arr)?.into_series()))
        })
    }

    pub fn utf8_lpad(&self, length: &Series, pad: &Series) -> DaftResult<Series> {
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

    pub fn utf8_rpad(&self, length: &Series, pad: &Series) -> DaftResult<Series> {
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

    pub fn utf8_repeat(&self, n: &Series) -> DaftResult<Series> {
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

    pub fn utf8_like(&self, pattern: &Series) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.like(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_ilike(&self, pattern: &Series) -> DaftResult<Series> {
        self.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| Ok(arr.ilike(pattern_arr)?.into_series()))
        })
    }

    pub fn utf8_substr(&self, start: &Series, length: &Series) -> DaftResult<Series> {
        if self.data_type() == &DataType::Null {
            return Ok(self.clone());
        }
        if self.data_type() != &DataType::Utf8 {
            return Err(DaftError::TypeError(format!(
                "Substr not implemented for type {}",
                self.data_type()
            )));
        }

        // This part is only illustrative for now
        match start.data_type() {
            DataType::UInt32 => Ok(self
                .utf8()?
                .substr(start.u32()?, length.u32()?)?
                .into_series()),
            DataType::Int32 => Ok(self
                .utf8()?
                .substr(start.i32()?, length.i32()?)?
                .into_series()),
            DataType::UInt64 => Ok(self
                .utf8()?
                .substr(start.u64()?, length.u64()?)?
                .into_series()),
            DataType::Int64 => Ok(self
                .utf8()?
                .substr(start.i64()?, length.i64()?)?
                .into_series()),
            DataType::Int8 => Ok(self
                .utf8()?
                .substr(start.i8()?, length.i8()?)?
                .into_series()),
            DataType::UInt8 => Ok(self
                .utf8()?
                .substr(start.u8()?, length.u8()?)?
                .into_series()),
            DataType::Int16 => Ok(self
                .utf8()?
                .substr(start.i16()?, length.i16()?)?
                .into_series()),
            DataType::UInt16 => Ok(self
                .utf8()?
                .substr(start.u16()?, length.u16()?)?
                .into_series()),
            DataType::Int128 => Ok(self
                .utf8()?
                .substr(start.i128()?, length.i128()?)?
                .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Substr not implemented for type {dt}"
            ))),
        }
    }
}
