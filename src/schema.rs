use std::sync::Arc;

use indexmap::IndexMap;

use crate::{
    datatypes::DataType,
    datatypes::Field,
    error::{DaftError, DaftResult},
};

type SchemaRef = Arc<Schema>;

pub struct Schema {
    fields: indexmap::IndexMap<String, Field>,
}

impl Schema {
    pub fn new(fields: &[(String, DataType)]) -> Self {
        let mut map: IndexMap<String, Field> = indexmap::IndexMap::new();

        for (name, dt) in fields.iter() {
            map.insert(name.clone(), Field::new(name.clone(), dt.clone()));
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
