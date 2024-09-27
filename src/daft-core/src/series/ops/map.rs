use common_error::{DaftError, DaftResult};

use crate::{datatypes::DataType, series::Series};

impl Series {
    pub fn map_get(&self, key: &Self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Map(_) => self.map()?.map_get(key),
            dt => Err(DaftError::TypeError(format!(
                "map.get not implemented for {}",
                dt
            ))),
        }
    }
}
