use common_error::DaftResult;

use crate::{datatypes::DataType, series::Series};

impl Series {
    pub fn cast(&self, datatype: &DataType) -> DaftResult<Self> {
        let casted = self.inner.cast(datatype)?;
        if casted.name() == self.name() {
            Ok(casted)
        } else {
            Ok(casted.rename(self.name()))
        }
    }
}
