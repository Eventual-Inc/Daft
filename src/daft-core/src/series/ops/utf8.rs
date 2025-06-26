use common_error::{DaftError, DaftResult};

use crate::{datatypes::*, series::Series};

impl Series {
    pub fn with_utf8_array(&self, f: impl Fn(&Utf8Array) -> DaftResult<Self>) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Utf8 => f(self.utf8()?),
            DataType::Null => Ok(self.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Operation not implemented for type {dt}"
            ))),
        }
    }
}
