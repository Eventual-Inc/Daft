use crate::{datatypes::DataType, error::DaftResult, series::Series};

impl Series {
    pub fn cast(&self, datatype: &DataType) -> DaftResult<Series> {
        self.inner.cast(datatype)
    }
}
