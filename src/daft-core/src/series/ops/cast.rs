use common_error::DaftResult;

use crate::{datatypes::DataType, series::Series};

impl Series {
    pub fn cast(&self, datatype: &DataType) -> DaftResult<Series> {
        self.inner.cast(datatype)
    }
}
