use common_error::DaftResult;

use crate::{datatypes::DataType, series::Series};

impl Series {
    pub fn cast(&self, datatype: &DataType) -> DaftResult<Self> {
        self.inner.cast(datatype)
    }
}
