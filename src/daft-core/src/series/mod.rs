mod array_impl;
mod from;
mod ops;
mod serdes;
mod series_like;
use std::{
    borrow::Cow,
    fmt::{Display, Formatter, Result},
    sync::Arc,
};

use crate::{
    array::{
        ops::{from_arrow::FromArrow, full::FullNull, DaftCompare},
        DataArray,
    },
    datatypes::{DaftDataType, DaftNumericType, DataType, Field, FieldRef, NumericNative},
    utils::display_table::make_comfy_table,
    with_match_daft_types,
};
use common_error::DaftResult;

pub use array_impl::IntoSeries;
pub use ops::cast_series_to_supertype;

pub(crate) use self::series_like::SeriesLike;

#[derive(Clone, Debug)]
pub struct Series {
    pub inner: Arc<dyn SeriesLike>,
}

impl PartialEq for Series {
    fn eq(&self, other: &Self) -> bool {
        match self.equal(other) {
            Ok(arr) => arr.into_iter().all(|x| x.unwrap_or(false)),
            Err(_) => false,
        }
    }
}

impl Series {
    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        self.inner.to_arrow()
    }

    /// Creates a Series given an Arrow [`arrow2::array::Array`]
    ///
    /// This function will check the provided [`Field`] (and all its associated potentially nested fields/dtypes) against
    /// the provided [`arrow2::array::Array`] for compatibility, and returns an error if they do not match.
    pub fn from_arrow(
        field: FieldRef,
        arrow_arr: Box<dyn arrow2::array::Array>,
    ) -> DaftResult<Self> {
        with_match_daft_types!(field.dtype, |$T| {
            Ok(<<$T as DaftDataType>::ArrayType as FromArrow>::from_arrow(field, arrow_arr)?.into_series())
        })
    }

    /// Creates a Series that is all nulls
    pub fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        with_match_daft_types!(dtype, |$T| {
            <<$T as DaftDataType>::ArrayType as FullNull>::full_null(name, dtype, length).into_series()
        })
    }

    /// Creates an empty [`Series`]
    pub fn empty(field_name: &str, dtype: &DataType) -> Self {
        with_match_daft_types!(dtype, |$T| {
            <<$T as DaftDataType>::ArrayType as FullNull>::empty(field_name, dtype).into_series()
        })
    }

    pub fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn rename<S: AsRef<str>>(&self, name: S) -> Self {
        self.inner.rename(name.as_ref())
    }

    pub fn field(&self) -> &Field {
        self.inner.field()
    }
    pub fn as_physical(&self) -> DaftResult<Series> {
        let physical_dtype = self.data_type().to_physical();
        if &physical_dtype == self.data_type() {
            Ok(self.clone())
        } else {
            self.inner.cast(&physical_dtype)
        }
    }

    pub fn to_comfy_table(&self) -> comfy_table::Table {
        make_comfy_table(
            vec![Cow::Borrowed(self.field())].as_slice(),
            Some([self].as_slice()),
            Some(80),
        )
    }

    pub fn with_validity(&self, validity: Option<arrow2::bitmap::Bitmap>) -> DaftResult<Series> {
        self.inner.with_validity(validity)
    }

    pub fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        self.inner.validity()
    }

    /// Attempts to downcast the Series to a primitive slice
    /// This will return an error if the Series is not of the physical type `T`
    /// # Example
    /// ```rust,no_run
    /// let i32_arr: &[i32] = series.try_as_slice::<i32>()?;
    ///
    /// let f64_arr: &[f64] = series.try_as_slice::<f64>()?;
    /// ```
    pub fn try_as_slice<N: NumericNative>(
        &self,
    ) -> DaftResult<&[<N::DAFTTYPE as DaftNumericType>::Native]>
    where
        N::DAFTTYPE: DaftNumericType + DaftDataType,
    {
        let data: &DataArray<N::DAFTTYPE> = self.downcast()?;
        Ok(data.as_slice())
    }
}
impl Display for Series {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        let table = self.to_comfy_table();
        writeln!(f, "{table}")
    }
}

#[cfg(test)]
mod tests {}
