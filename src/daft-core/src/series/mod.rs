mod array_impl;
mod from;
mod ops;
mod series_like;

use std::{
    fmt::{Display, Formatter, Result},
    sync::Arc,
};

use crate::datatypes::{DataType, Field};
use common_error::DaftResult;

pub use array_impl::IntoSeries;

use self::series_like::SeriesLike;

#[derive(Clone)]
pub struct Series {
    pub inner: Arc<dyn SeriesLike>,
}

impl Series {
    // Copies and exports this Series for exporting as an Arrow array
    //
    // This should mostly be called only when preparing data for crossing the FFI boundary into Python
    // and has custom handling for some types to be compatible with Python
    pub fn export_arrow_for_ffi(&self) -> Box<dyn arrow2::array::Array> {
        self.inner.export_arrow_for_ffi()
    }

    // Retrieves the Arrow representation of this Series for internal use in kernels.
    //
    // Note that the Arrow array returned here will be the physical representation of the data,
    // since not all Daft logical types have a direct analog in arrow2 (e.g. Extension types).
    //
    // This behavior is in contrast with `Series::export_arrow_for_ffi` which performs manipulations
    // of the data to prepare it for crossing the FFI boundary.
    pub fn as_arrow(&self) -> &dyn arrow2::array::Array {
        self.inner.as_arrow()
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
