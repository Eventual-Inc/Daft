use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::PyTypeInfo;

use serde::{Deserialize, Serialize};

use super::datatype::PyDataType;
use super::field::PyField;
use crate::datatypes;
use crate::impl_bincode_py_state_serialization;
use crate::schema;

#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PySchema {
    pub schema: schema::SchemaRef,
}

#[pymethods]
impl PySchema {
    pub fn __getitem__(&self, name: &str) -> PyResult<PyField> {
        Ok(self.schema.get_field(name)?.clone().into())
    }

    pub fn names(&self) -> Vec<String> {
        self.schema.names()
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
        let schema = schema::Schema::new(fields)?;
        Ok(PySchema {
            schema: schema.into(),
        })
    }

    #[staticmethod]
    pub fn from_fields(fields: Vec<PyField>) -> PyResult<PySchema> {
        Ok(PySchema {
            schema: schema::Schema::new(fields.iter().map(|f| f.field.clone()).collect())?.into(),
        })
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.schema))
    }

    pub fn _repr_html_(&self) -> PyResult<String> {
        Ok(self.schema.repr_html())
    }
}

impl_bincode_py_state_serialization!(PySchema);

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
