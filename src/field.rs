use crate::datatypes::DataType;

pub struct Field {
    pub name: String,
    pub dtype: DataType,
}

impl Field {
    pub fn new(name: &str, dtype: DataType) -> Self {
        Field {
            name: name.to_string(),
            dtype: dtype,
        }
    }
}
