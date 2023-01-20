use std::sync::Arc;

use indexmap::IndexMap;

use crate::{
    datatypes::Field,
    error::{DaftError, DaftResult},
};

type SchemaRef = Arc<Schema>;

pub struct Schema {
    pub fields: indexmap::IndexMap<String, Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        let mut map: IndexMap<String, Field> = indexmap::IndexMap::new();

        for f in fields.into_iter() {
            map.insert(f.name.clone(), f);
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

// impl From<&[Field]> for Schema {
//     fn from(slice: &[bool]) -> Self {
//     }
// }
