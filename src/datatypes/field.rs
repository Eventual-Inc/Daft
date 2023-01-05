use arrow2::datatypes::Field as ArrowField;

use crate::{datatypes::dtype::DataType, error::DaftResult};
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
    pub fn to_arrow(&self) -> DaftResult<ArrowField> {
        Ok(ArrowField::new(
            self.name.clone(),
            self.dtype.to_arrow()?,
            true,
        ))
    }
}

impl From<&ArrowField> for Field {
    fn from(af: &ArrowField) -> Self {
        Field {
            name: af.name.clone(),
            dtype: af.data_type().into(),
        }
    }
}
