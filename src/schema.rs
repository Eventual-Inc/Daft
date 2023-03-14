use std::{
    collections::HashSet,
    fmt::{Display, Formatter, Result},
    sync::Arc,
};

use indexmap::IndexMap;

use crate::{
    datatypes::Field,
    error::{DaftError, DaftResult},
};

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, PartialEq)]
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
            None => Err(DaftError::NotFound(format!(
                "Field: {} not found in {:?}",
                name,
                self.fields.values()
            ))),
            Some(val) => Ok(val),
        }
    }

    pub fn get_index(&self, name: &str) -> DaftResult<usize> {
        match self.fields.get_index_of(name) {
            None => Err(DaftError::NotFound(format!(
                "Field: {} not found in {:?}",
                name,
                self.fields.values()
            ))),
            Some(val) => Ok(val),
        }
    }

    pub fn names(&self) -> DaftResult<Vec<String>> {
        return Ok(self.fields.keys().cloned().collect());
    }

    pub fn union(&self, other: &Schema) -> DaftResult<Schema> {
        let self_keys: HashSet<&String> = HashSet::from_iter(self.fields.keys());
        let other_keys: HashSet<&String> = HashSet::from_iter(self.fields.keys());
        match self_keys.difference(&other_keys).count() {
            0 => {
                let mut fields = IndexMap::new();
                for (k, v) in self.fields.iter().chain(other.fields.iter()) {
                    fields.insert(k.clone(), v.clone());
                }
                Ok(Schema { fields })
            }
            _ => Err(DaftError::ValueError(
                "Cannot union two schemas with overlapping keys".to_string(),
            )),
        }
    }
}

impl Display for Schema {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        let mut table = prettytable::Table::new();

        let header = self
            .fields
            .iter()
            .map(|(name, field)| format!("{}\n{:?}", name, field.dtype))
            .collect();
        table.add_row(header);
        write!(f, "{table}")
    }
}
