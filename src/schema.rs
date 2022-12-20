use indexmap::IndexMap;
use std::sync::Arc;

use crate::{
    error::{DaftError, DaftResult},
    field::Field,
};

type SchemaRef = Arc<Schema>;

pub struct Schema {
    fields: indexmap::IndexMap<String, Field>,
}

impl Schema {
    pub fn new() -> Schema {
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
}
