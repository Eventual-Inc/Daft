use std::{
    collections::HashSet,
    fmt::{Display, Formatter, Result},
    sync::Arc,
};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::datatypes::Field;

use common_error::{DaftError, DaftResult};

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Schema {
    #[serde(with = "indexmap::serde_seq")]
    pub fields: indexmap::IndexMap<String, Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> DaftResult<Self> {
        let mut map: IndexMap<String, Field> = indexmap::IndexMap::new();

        for f in fields.into_iter() {
            let old = map.insert(f.name.clone(), f);
            if let Some(item) = old {
                return Err(DaftError::ValueError(format!(
                    "Attempting to make a Schema with duplicate field names: {}",
                    item.name
                )));
            }
        }

        Ok(Schema { fields: map })
    }
    pub fn empty() -> Self {
        Schema {
            fields: indexmap::IndexMap::new(),
        }
    }

    pub fn get_field(&self, name: &str) -> DaftResult<&Field> {
        match self.fields.get(name) {
            None => Err(DaftError::FieldNotFound(format!(
                "Field: {} not found in {:?}",
                name,
                self.fields.values()
            ))),
            Some(val) => Ok(val),
        }
    }

    pub fn get_index(&self, name: &str) -> DaftResult<usize> {
        match self.fields.get_index_of(name) {
            None => Err(DaftError::FieldNotFound(format!(
                "Field: {} not found in {:?}",
                name,
                self.fields.values()
            ))),
            Some(val) => Ok(val),
        }
    }

    pub fn names(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
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

    pub fn to_arrow(&self) -> DaftResult<arrow2::datatypes::Schema> {
        let arrow_fields: DaftResult<Vec<arrow2::datatypes::Field>> =
            self.fields.iter().map(|(_, f)| f.to_arrow()).collect();
        let arrow_fields = arrow_fields?;
        Ok(arrow2::datatypes::Schema {
            fields: arrow_fields,
            metadata: Default::default(),
        })
    }

    pub fn repr_html(&self) -> String {
        // Produces a <table> HTML element.

        let mut res = "<table class=\"dataframe\">\n".to_string();

        // Begin the header.
        res.push_str("<thead><tr>");

        for (name, field) in &self.fields {
            res.push_str("<th style=\"text-wrap: nowrap; max-width:192px; overflow:auto\">");
            res.push_str(&html_escape::encode_text(name));
            res.push_str("<br />");
            res.push_str(&html_escape::encode_text(&format!("{}", field.dtype)));
            res.push_str("</th>");
        }

        // End the header.
        res.push_str("</tr></thead>\n");

        res.push_str("</table>");

        res
    }

    pub fn short_string(&self) -> String {
        self.fields
            .iter()
            .map(|(name, field)| format!("{} ({:?})", name, field.dtype))
            .collect::<Vec<String>>()
            .join(", ")
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::empty()
    }
}

impl Display for Schema {
    // Produces an ASCII table.
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

impl TryFrom<&arrow2::datatypes::Schema> for Schema {
    type Error = DaftError;
    fn try_from(arrow_schema: &arrow2::datatypes::Schema) -> DaftResult<Self> {
        let fields = &arrow_schema.fields;
        let daft_fields: Vec<Field> = fields.iter().map(|f| f.into()).collect();
        Self::new(daft_fields)
    }
}
