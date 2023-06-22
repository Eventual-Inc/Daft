use crate::{datatypes::DataType, series::Series};
use common_error::DaftResult;

impl Series {
    pub fn cast(&self, datatype: &DataType) -> DaftResult<Series> {
        self.inner.cast(datatype)
    }
}
