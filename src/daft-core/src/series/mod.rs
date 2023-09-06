mod array_impl;
mod from;
mod ops;
mod series_like;

use std::{
    fmt::{Display, Formatter, Result},
    sync::Arc,
};

use crate::{
    array::ops::{from_arrow::FromArrow, full::FullNull},
    datatypes::{DataType, Field},
    with_match_daft_types,
};
use common_error::DaftResult;

pub use array_impl::IntoSeries;

pub(crate) use self::series_like::SeriesLike;

#[derive(Clone)]
pub struct Series {
    pub inner: Arc<dyn SeriesLike>,
}

impl Series {
    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        self.inner.to_arrow()
    }

    /// Creates a Series given an Arrow [`arrow2::array::Array`]
    ///
    /// This function will check the provided [`Field`] (and all its associated potentially nested fields/dtypes) against
    /// the provided [`arrow2::array::Array`] for compatibility, and returns an error if they do not match.
    pub fn from_arrow(field: &Field, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
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

    pub fn to_prettytable(&self) -> prettytable::Table {
        let mut table = prettytable::Table::new();

        let header =
            prettytable::Cell::new(format!("{}\n{}", self.name(), self.data_type()).as_str())
                .with_style(prettytable::Attr::Bold);
        table.add_row(prettytable::Row::new(vec![header]));

        let head_rows;
        let tail_rows;

        if self.len() > 10 {
            head_rows = 5;
            tail_rows = 5;
        } else {
            head_rows = self.len();
            tail_rows = 0;
        }

        for i in 0..head_rows {
            let row = vec![self.str_value(i).unwrap()];
            table.add_row(row.into());
        }
        if tail_rows != 0 {
            let row = vec!["..."];
            table.add_row(row.into());
        }

        for i in 0..tail_rows {
            let row = vec![self.str_value(self.len() - tail_rows - 1 + i).unwrap()];
            table.add_row(row.into());
        }

        table
    }
}

impl Display for Series {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        let table = self.to_prettytable();
        write!(f, "{table}")
    }
}

#[cfg(test)]
mod tests {}
