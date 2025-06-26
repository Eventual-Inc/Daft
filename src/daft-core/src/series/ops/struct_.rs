use common_error::{DaftError, DaftResult};

use crate::{datatypes::DataType, series::Series};

impl Series {
    pub fn struct_get(&self, name: &str) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Struct(_) => self.struct_()?.get(name),
            dt => Err(DaftError::TypeError(format!(
                "get not implemented for {}",
                dt
            ))),
        }
    }
}
