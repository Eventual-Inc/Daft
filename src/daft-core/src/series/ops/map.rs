use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn map_get(&self, key: &Series) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Map(_) => self.map()?.map_get(key),
            dt => Err(DaftError::TypeError(format!(
                "map.get not implemented for {}",
                dt
            ))),
        }
    }
}
