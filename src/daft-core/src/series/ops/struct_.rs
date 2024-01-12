use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn struct_get(&self, name: &str) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Struct(_) => self.struct_()?.get(name),
            dt => Err(DaftError::TypeError(format!(
                "get not implemented for {}",
                dt
            ))),
        }
    }
}
