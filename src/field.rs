use crate::datatypes::DataType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub dtype: DataType,
}

impl Field {
    pub fn new(name: &str, dtype: DataType) -> Self {
        Field {
            name: name.to_string(),
            dtype,
        }
    }
}
