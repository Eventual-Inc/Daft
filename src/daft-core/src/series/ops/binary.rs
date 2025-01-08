use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::*,
    series::{array_impl::IntoSeries, Series},
    with_match_integer_daft_types,
};

impl Series {
    pub fn with_binary_array(
        &self,
        f: impl Fn(&BinaryArray) -> DaftResult<Self>,
    ) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Binary => f(self.binary()?),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Operation not implemented for type {dt}"
            ))),
        }
    }

    pub fn binary_length(&self) -> DaftResult<Self> {
        self.with_binary_array(|arr| Ok(arr.length()?.into_series()))
    }

    pub fn binary_concat(&self, other: &Self) -> DaftResult<Self> {
        self.with_binary_array(|arr| Ok(arr.binary_concat(other.binary()?)?.into_series()))
    }

    pub fn binary_substr(&self, start: &Self, length: &Self) -> DaftResult<Self> {
        self.with_binary_array(|arr| {
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
        })
    }
}
