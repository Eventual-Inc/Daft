use std::{
    borrow::Cow,
    collections::{hash_map::DefaultHasher, HashSet},
    fmt::{Display, Formatter, Result},
    hash::{Hash, Hasher},
    sync::Arc,
};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{
    datatypes::Field,
    utils::display_table::{make_comfy_table, make_schema_vertical_table},
};

use common_error::{DaftError, DaftResult};

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Schema {
    #[serde(with = "indexmap::map::serde_seq")]
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

    pub fn exclude<S: AsRef<str>>(&self, names: &[S]) -> DaftResult<Schema> {
        let mut fields = IndexMap::new();
        let names = names.iter().map(|s| s.as_ref()).collect::<HashSet<&str>>();
        for (name, field) in self.fields.iter() {
            if !names.contains(&name.as_str()) {
                fields.insert(name.clone(), field.clone());
            }
        }

        Ok(Schema { fields })
    }

    pub fn empty() -> Self {
        Schema {
            fields: indexmap::IndexMap::new(),
        }
    }

    pub fn get_field(&self, name: &str) -> DaftResult<&Field> {
        match self.fields.get(name) {
            None => Err(DaftError::FieldNotFound(format!(
                "Column \"{}\" not found in schema: {:?}",
                name,
                self.fields.keys()
            ))),
            Some(val) => Ok(val),
        }
    }

    pub fn get_index(&self, name: &str) -> DaftResult<usize> {
        match self.fields.get_index_of(name) {
            None => Err(DaftError::FieldNotFound(format!(
                "Column \"{}\" not found in schema: {:?}",
                name,
                self.fields.keys()
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

    pub fn apply_hints(&self, hints: &Schema) -> DaftResult<Schema> {
        let applied_fields = self
            .fields
            .iter()
            .map(|(name, field)| match hints.fields.get(name) {
                None => (name.clone(), field.clone()),
                Some(hint_field) => (name.clone(), hint_field.clone()),
            })
            .collect::<IndexMap<String, Field>>();

        Ok(Schema {
            fields: applied_fields,
        })
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

        // Add header for column name and type
        res.push_str("<th style=\"text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left\">Column Name</th>");
        res.push_str("<th style=\"text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left\">Type</th>");

        // End the header.
        res.push_str("</tr></thead>\n");

        // Begin the body.
        res.push_str("<tbody>\n");

        for (name, field) in &self.fields {
            res.push_str("<tr>");
            res.push_str(
                "<td style=\"text-align:left; max-width:192px; max-height:64px; overflow:auto\">",
            );
            res.push_str(&html_escape::encode_text(name));
            res.push_str("</td>");
            res.push_str(
                "<td style=\"text-align:left; max-width:192px; max-height:64px; overflow:auto\">",
            );
            res.push_str(&html_escape::encode_text(&format!("{}", field.dtype)));
            res.push_str("</td>");
            res.push_str("</tr>\n");
        }

        // End the body.
        res.push_str("</tbody>\n");

        res.push_str("</table>");

        res
    }

    pub fn truncated_table_html(&self) -> String {
        // Produces a <table> HTML element.

        let mut res = "<table class=\"dataframe\">\n".to_string();

        // Begin the header.
        res.push_str("<thead><tr>");

        for (name, field) in &self.fields {
            res.push_str(
                "<th style=\"text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left\">",
            );
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
            .map(|(name, field)| format!("{}#{:?}", name, field.dtype))
            .collect::<Vec<String>>()
            .join(", ")
    }

    pub fn truncated_table_string(&self) -> String {
        let table = make_comfy_table(
            self.fields
                .values()
                .map(Cow::Borrowed)
                .collect::<Vec<_>>()
                .as_slice(),
            None,
            None,
        );
        format!("{}\n", table)
    }

    pub fn estimate_row_size_bytes(&self) -> f64 {
        self.fields
            .values()
            .map(|f| f.dtype.estimate_size_bytes().unwrap_or(0.))
            .sum()
    }

    /// Returns a new schema with only the specified columns in the new schema
    pub fn project<S: AsRef<str>>(self: Arc<Self>, columns: &[S]) -> DaftResult<Schema> {
        let new_fields = columns
            .iter()
            .map(|i| {
                let key = i.as_ref();
                self.fields.get(key).cloned().ok_or_else(|| {
                    DaftError::SchemaMismatch(format!(
                        "Column {} not found in schema: {:?}",
                        key, self.fields
                    ))
                })
            })
            .collect::<DaftResult<Vec<_>>>()?;
        Self::new(new_fields)
    }
}

impl Eq for Schema {}

impl Hash for Schema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(hash_index_map(&self.fields))
    }
}

pub fn hash_index_map<K: Hash, V: Hash>(indexmap: &indexmap::IndexMap<K, V>) -> u64 {
    // Must preserve x == y --> hash(x) == hash(y).
    // Since IndexMap implements order-independent equality, we must implement an order-independent hashing scheme.
    // We achieve this by combining the hashes of key-value pairs with an associative + commutative operation so
    // order does not matter, i.e. (u64, *, 0) must form a commutative monoid. This is satisfied by * = u64::wrapping_add.
    //
    // Moreover, the hashing of each individual element must be independent of the hashing of other elements, so we hash
    // each element with a fresh state (hasher).
    //
    // NOTE: This is a relatively weak hash function, but should be fine for our current hashing use case, which is detecting
    // logical optimization cycles in the optimizer.
    indexmap
        .iter()
        .map(|kv| {
            let mut h = DefaultHasher::new();
            kv.hash(&mut h);
            h.finish()
        })
        .fold(0, u64::wrapping_add)
}

impl Default for Schema {
    fn default() -> Self {
        Self::empty()
    }
}

impl Display for Schema {
    // Produces an ASCII table.
    fn fmt(&self, f: &mut Formatter) -> Result {
        let table = make_schema_vertical_table(
            self.fields
                .values()
                .map(Cow::Borrowed)
                .collect::<Vec<_>>()
                .as_slice(),
        );
        writeln!(f, "{table}")
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
