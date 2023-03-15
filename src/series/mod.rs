mod from;
mod ops;

use std::{
    fmt::{Display, Formatter, Result},
    sync::Arc,
};

use crate::{
    array::BaseArray,
    datatypes::{DataType, Field},
    error::DaftResult,
};

#[derive(Debug, Clone)]
pub struct Series {
    data_array: Arc<dyn BaseArray>,
}

impl Series {
    pub fn new(data_array: Arc<dyn BaseArray>) -> Self {
        Series { data_array }
    }

    pub fn array(&self) -> &dyn BaseArray {
        self.data_array.as_ref()
    }

    pub fn data_type(&self) -> &DataType {
        self.data_array.data_type()
    }

    pub fn name(&self) -> &str {
        self.data_array.name()
    }

    pub fn rename<S: AsRef<str>>(&self, name: S) -> Self {
        Self::new(Arc::from(self.data_array.rename(name.as_ref())))
    }

    pub fn field(&self) -> &Field {
        self.data_array.field()
    }

    pub fn as_physical(&self) -> DaftResult<Series> {
        let physical_dtype = self.data_type().to_physical();
        if &physical_dtype == self.data_type() {
            Ok(self.clone())
        } else {
            self.cast(&physical_dtype)
        }
    }
}

impl Display for Series {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        let mut table = prettytable::Table::new();

        let header =
            prettytable::Cell::new(format!("{}\n{:?}", self.name(), self.data_type()).as_str())
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

        write!(f, "{table}")
    }
}

#[cfg(test)]
mod tests {}
