use std::sync::Arc;

use pyo3::prelude::*;

use super::datatype::PyDataType;
use super::field::PyField;
use crate::datatypes;
use crate::schema;

#[pyclass]
pub struct PySchema {
    pub schema: schema::SchemaRef,
}

#[pymethods]
impl PySchema {
    pub fn __getitem__(&self, name: &str) -> PyResult<PyField> {
        Ok(self.schema.get_field(name)?.clone().into())
    }

    pub fn names(&self) -> PyResult<Vec<String>> {
        Ok(self.schema.names()?)
    }

    pub fn union(&self, other: &PySchema) -> PyResult<PySchema> {
        let new_schema = Arc::new(self.schema.union(&other.schema)?);
        Ok(new_schema.into())
    }

    pub fn eq(&self, other: &PySchema) -> PyResult<bool> {
        Ok(self.schema.fields.eq(&other.schema.fields))
    }

    #[staticmethod]
    pub fn from_field_name_and_types(
        names_and_types: Vec<(String, PyDataType)>,
    ) -> PyResult<PySchema> {
        let fields = names_and_types
            .iter()
            .map(|(name, pydtype)| datatypes::Field::new(name, pydtype.clone().into()))
            .collect();
        let schema = schema::Schema::new(fields);
        Ok(PySchema {
            schema: schema.into(),
        })
    }
}

impl From<schema::SchemaRef> for PySchema {
    fn from(schema: schema::SchemaRef) -> Self {
        PySchema { schema }
    }
}

impl From<PySchema> for schema::SchemaRef {
    fn from(item: PySchema) -> Self {
        item.schema
    }
}
