use indexmap::IndexMap;
use std::sync::Arc;

use crate::{
    datatypes::dtype::DataType,
    datatypes::field::Field,
    error::{DaftError, DaftResult},
};

type SchemaRef = Arc<Schema>;

pub struct Schema {
    fields: indexmap::IndexMap<String, Field>,
}

impl Schema {
    pub fn new(fields: &[(String, DataType)]) -> Self {
        let mut map = indexmap::IndexMap::new();

        for (name, dt) in fields.iter() {
            map.insert(
                name.clone(),
                Field {
                    name: name.clone(),
                    dtype: dt.clone(),
                },
            );
        }

        Schema { fields: map }
    }
    pub fn empty() -> Self {
        Schema {
            fields: indexmap::IndexMap::new(),
        }
    }

    pub fn get_field(&self, name: &str) -> DaftResult<&Field> {
        match self.fields.get(name) {
            None => Err(DaftError::NotFound(name.into())),
            Some(val) => Ok(val),
        }
    }

    pub fn get_index(&self, name: &str) -> DaftResult<usize> {
        match self.fields.get_index_of(name) {
            None => Err(DaftError::NotFound(name.into())),
            Some(val) => Ok(val),
        }
    }
}
