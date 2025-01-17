use common_error::{DaftError, DaftResult};

use crate::{datatypes::*, series::Series};

impl Series {
    pub fn with_binary_array(
        &self,
        f: impl Fn(&BinaryArray) -> DaftResult<Self>,
    ) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Binary => f(self.binary()?),
            DataType::FixedSizeBinary(_) => Err(DaftError::TypeError(format!(
                "Operation not implemented for type {}",
                self.data_type()
            ))),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Operation not implemented for type {dt}"
            ))),
        }
    }
}
