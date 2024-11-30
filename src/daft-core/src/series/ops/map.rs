use common_error::{DaftError, DaftResult};

use crate::{datatypes::DataType, series::Series};

impl Series {
    pub fn map_get(&self, key: &Self) -> DaftResult<Self> {
        let DataType::Map { .. } = self.data_type() else {
            return Err(DaftError::TypeError(format!(
                "map.get not implemented for {}",
                self.data_type()
            )));
        };

        self.map()?.map_get(key)
    }
}
