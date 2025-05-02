use std::{
    collections::{HashMap, HashSet},
    ops::Index,
    sync::Arc,
};

use common_display::{
    table_display::{make_comfy_table, make_schema_vertical_table},
    DisplayAs,
};
use common_error::{DaftError, DaftResult};
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::{field::Field, prelude::DataType};

pub type SchemaRef = Arc<Schema>;

use derivative::Derivative;

#[derive(Debug, Display, Serialize, Deserialize, Derivative, Eq)]
#[derivative(Hash, PartialEq)]
#[display("{}\n", make_schema_vertical_table(
    self.fields.iter().map(|field| (field.name.clone(), field.dtype.to_string()))
))]
pub struct Schema {
    fields: Vec<Field>,

    #[derivative(Hash = "ignore", PartialEq = "ignore")]
    name_to_indices: HashMap<String, Vec<usize>>,
}

impl Schema {
    pub fn new<I, F>(fields: I) -> Self
    where
        I: IntoIterator<Item = F>,
        F: Into<Field>,
    {
        let mut name_to_indices = HashMap::<String, Vec<usize>>::new();

        let field_vec = fields
            .into_iter()
            .enumerate()
            .map(|(idx, field)| {
                let field = field.into();

                if let Some(indices) = name_to_indices.get_mut(&field.name) {
                    indices.push(idx);
                } else {
                    name_to_indices.insert(field.name.clone(), vec![idx]);
                }

                field
            })
            .collect();

        Self {
            fields: field_vec,
            name_to_indices,
        }
    }

    pub fn empty() -> Self {
        Self {
            fields: vec![],
            name_to_indices: HashMap::new(),
        }
    }

    pub fn to_struct(&self) -> DataType {
        DataType::Struct(self.fields.clone())
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.name.as_str())
    }

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn exclude<S: AsRef<str>>(&self, names: &[S]) -> Self {
        let names = names.iter().map(|s| s.as_ref()).collect::<HashSet<&str>>();
        let fields = self
            .fields
            .iter()
            .filter(|field| !names.contains(field.name.as_str()))
            .cloned()
            .collect::<Vec<_>>();

        Self::new(fields)
    }

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn get_field(&self, name: &str) -> DaftResult<&Field> {
        if let Some(indices) = self.name_to_indices.get(name) {
            if let [idx] = indices.as_slice() {
                Ok(&self.fields[*idx])
            } else {
                Err(DaftError::AmbiguousReference(format!(
                    "Column name \"{}\" is ambiguous in schema: {:?}",
                    name, self.fields
                )))
            }
        } else {
            Err(DaftError::FieldNotFound(format!(
                "Column \"{}\" not found in schema: {:?}",
                name, self.fields
            )))
        }
    }

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn has_field(&self, name: &str) -> bool {
        self.name_to_indices.contains_key(name)
    }

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn get_index(&self, name: &str) -> DaftResult<usize> {
        if let Some(indices) = self.name_to_indices.get(name) {
            if let [idx] = indices.as_slice() {
                Ok(*idx)
            } else {
                Err(DaftError::AmbiguousReference(format!(
                    "Column name \"{}\" is ambiguous in schema: {:?}",
                    name, self.fields
                )))
            }
        } else {
            Err(DaftError::FieldNotFound(format!(
                "Column \"{}\" not found in schema: {:?}",
                name, self.fields
            )))
        }
    }

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn names(&self) -> Vec<String> {
        self.fields.iter().map(|f| &f.name).cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Takes the disjoint union over the `self` and `other` schemas, throwing an error if the
    /// schemas contain overlapping keys.
    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn union(&self, other: &Self) -> DaftResult<Self> {
        for other_name in other.name_to_indices.keys() {
            if self.name_to_indices.contains_key(other_name) {
                return Err(DaftError::ValueError(
                    "Cannot disjoint union two schemas with overlapping keys".to_string(),
                ));
            }
        }

        Ok(Self::new(
            self.fields.iter().chain(other.fields.iter()).cloned(),
        ))
    }

    /// Takes the non-distinct union of two schemas. If there are overlapping keys, then we take the
    /// corresponding position from `self` and field from `other`.
    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn non_distinct_union(&self, other: &Self) -> DaftResult<Self> {
        let fields = self.fields.iter().map(|f| {
            if let Some(indices) = other.name_to_indices.get(&f.name) {
                if let [idx] = indices.as_slice() {
                    Ok(other.fields[*idx].clone())
                } else {
                    Err(DaftError::InternalError(format!("Attempted to non-distinct union two schemas, but right schema has duplicate column name: {}", f.name)))
                }
            } else {
                Ok(f.clone())
            }
        }).chain(other.fields.iter().filter(|f| {
            !self.name_to_indices.contains_key(&f.name)
        }).cloned().map(Ok)).collect::<DaftResult<Vec<_>>>()?;

        Ok(Self::new(fields))
    }

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn apply_hints(&self, hints: &Self) -> DaftResult<Self> {
        let applied_fields = self
            .fields
            .iter()
            .map(|f| {
                if let Some(indices) = hints.name_to_indices.get(&f.name) {
                    if let [idx] = indices.as_slice() {
                        Ok(hints.fields[*idx].clone())
                    } else {
                        Err(DaftError::AmbiguousReference(format!(
                            "Attempted to apply hint schema with ambiguous column name \"{}\": {}",
                            f.name, hints
                        )))
                    }
                } else {
                    Ok(f.clone())
                }
            })
            .collect::<DaftResult<_>>()?;

        Ok(Self {
            fields: applied_fields,
            name_to_indices: self.name_to_indices.clone(),
        })
    }

    pub fn to_arrow(&self) -> DaftResult<arrow2::datatypes::Schema> {
        let arrow_fields: DaftResult<Vec<arrow2::datatypes::Field>> =
            self.fields.iter().map(Field::to_arrow).collect();
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
        res.push_str("<th style=\"text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left\">column_name</th>");
        res.push_str("<th style=\"text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left\">type</th>");

        // End the header.
        res.push_str("</tr></thead>\n");

        // Begin the body.
        res.push_str("<tbody>\n");

        for field in &self.fields {
            res.push_str("<tr>");
            res.push_str(
                "<td style=\"text-align:left; max-width:192px; max-height:64px; overflow:auto\">",
            );
            res.push_str(&html_escape::encode_text(&field.name));
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

        for field in &self.fields {
            res.push_str(
                "<th style=\"text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left\">",
            );
            res.push_str(&html_escape::encode_text(&field.name));
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
        if self.is_empty() {
            return "EMPTY".to_string();
        }
        self.fields
            .iter()
            .map(|field| format!("{}#{:?}", field.name, field.dtype))
            .collect::<Vec<String>>()
            .join(", ")
    }

    pub fn truncated_table_string(&self) -> String {
        let table = make_comfy_table(
            self.fields
                .iter()
                .map(|field| format!("{}\n---\n{}", field.name, field.dtype))
                .collect::<Vec<_>>()
                .as_slice(),
            None,
            None,
            None,
        );
        format!("{}\n", table)
    }

    pub fn estimate_row_size_bytes(&self) -> f64 {
        self.fields
            .iter()
            .map(|f| f.dtype.estimate_size_bytes().unwrap_or(0.))
            .sum()
    }

    /// Returns a new schema with only the specified columns in the new schema
    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn project<S: AsRef<str>>(self: Arc<Self>, columns: &[S]) -> DaftResult<Self> {
        let new_fields = columns
            .iter()
            .map(|i| {
                let key = i.as_ref();

                if let Some(indices) = self.name_to_indices.get(key) {
                    if let [idx] = indices.as_slice() {
                        Ok(self.fields[*idx].clone())
                    } else {
                        Err(DaftError::AmbiguousReference(format!(
                            "Column name {} is ambiguous in schema: {:?}",
                            key, self.fields
                        )))
                    }
                } else {
                    Err(DaftError::FieldNotFound(format!(
                        "Column {} not found in schema: {:?}",
                        key, self.fields
                    )))
                }
            })
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(Self::new(new_fields))
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::empty()
    }
}

impl DisplayAs for Schema {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        match level {
            common_display::DisplayLevel::Compact => self.short_string(),
            common_display::DisplayLevel::Default => self.truncated_table_string(),
            common_display::DisplayLevel::Verbose => self.to_string(),
        }
    }
}

impl From<arrow2::datatypes::Schema> for Schema {
    fn from(arrow_schema: arrow2::datatypes::Schema) -> Self {
        (&arrow_schema).into()
    }
}

impl From<&arrow2::datatypes::Schema> for Schema {
    fn from(arrow_schema: &arrow2::datatypes::Schema) -> Self {
        let daft_fields: Vec<Field> = arrow_schema.fields.iter().map(|f| f.into()).collect();
        Self::new(daft_fields)
    }
}

impl<'a> IntoIterator for &'a Schema {
    type Item = &'a Field;
    type IntoIter = std::slice::Iter<'a, Field>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields().iter()
    }
}

impl Index<usize> for Schema {
    type Output = Field;

    fn index(&self, i: usize) -> &Self::Output {
        &self.fields[i]
    }
}
