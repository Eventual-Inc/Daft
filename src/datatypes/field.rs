use std::fmt::{Display, Formatter, Result};

use arrow2::datatypes::Field as ArrowField;

use crate::{
    datatypes::dtype::DataType,
    error::{DaftError, DaftResult},
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct Field {
    pub name: String,
    pub dtype: DataType,
}

impl Field {
    pub fn new<S: Into<String>>(name: S, dtype: DataType) -> Self {
        Field {
            name: name.into(),
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
    pub fn rename<S: Into<String>>(&self, name: S) -> Self {
        Field::new(name, self.dtype.clone())
    }
    pub fn to_list_field(&self) -> DaftResult<Self> {
        if self.dtype.is_python() {
            return Err(DaftError::TypeError(format!(
                "Attempting to promote a Python Field: {self} into a List Field, which is currently not implemented."
            )));
        }
        let list_dtype = DataType::List(Box::new(self.clone()));
        Ok(Field {
            name: self.name.clone(),
            dtype: list_dtype,
        })
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

impl Display for Field {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}#{}", self.name, self.dtype)
    }
}
