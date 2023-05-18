use std::fmt::{Display, Formatter, Result};
use std::sync::Arc;

use arrow2::datatypes::Field as ArrowField;

use crate::{datatypes::dtype::DataType, error::DaftResult};

use serde::{Deserialize, Serialize};

pub type Metadata = std::collections::BTreeMap<String, String>;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct Field {
    pub name: String,
    pub dtype: DataType,
    pub metadata: Arc<Metadata>,
}

impl Field {
    pub fn new<S: Into<String>>(name: S, dtype: DataType) -> Self {
        Self {
            name: name.into(),
            dtype,
            metadata: Default::default(),
        }
    }

    pub fn with_metadata<M: Into<Arc<Metadata>>>(self, metadata: M) -> Self {
        Self {
            name: self.name,
            dtype: self.dtype,
            metadata: metadata.into(),
        }
    }

    pub fn to_arrow(&self) -> DaftResult<ArrowField> {
        Ok(
            ArrowField::new(self.name.clone(), self.dtype.to_arrow()?, true)
                .with_metadata(self.metadata.as_ref().clone()),
        )
    }

    pub fn rename<S: Into<String>>(&self, name: S) -> Self {
        Self {
            name: name.into(),
            dtype: self.dtype.clone(),
            metadata: self.metadata.clone(),
        }
    }

    pub fn to_list_field(&self) -> DaftResult<Self> {
        if self.dtype.is_python() {
            return Ok(self.clone());
        }
        let list_dtype = DataType::List(Box::new(self.clone()));
        Ok(Self {
            name: self.name.clone(),
            dtype: list_dtype,
            metadata: self.metadata.clone(),
        })
    }
}

impl From<&ArrowField> for Field {
    fn from(af: &ArrowField) -> Self {
        Self {
            name: af.name.clone(),
            dtype: af.data_type().into(),
            metadata: af.metadata.clone().into(),
        }
    }
}

impl Display for Field {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}#{}", self.name, self.dtype)
    }
}
