use std::sync::Arc;

use pyo3::prelude::*;

use super::datatype::PyDataType;
use crate::datatypes;
use crate::python::datatype;
use crate::schema;

#[pyclass]
pub struct PySchema {
    pub schema: schema::SchemaRef,
}

#[pyclass]
pub struct PyField {
    pub field: datatypes::Field,
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

#[pymethods]
impl PyField {
    pub fn name(&self) -> PyResult<String> {
        Ok(self.field.name.clone())
    }

    pub fn dtype(&self) -> PyResult<datatype::PyDataType> {
        Ok(self.field.dtype.clone().into())
    }

    pub fn eq(&self, other: &PyField) -> PyResult<bool> {
        Ok(self.field.eq(&other.field))
    }
}

impl From<datatypes::Field> for PyField {
    fn from(field: datatypes::Field) -> Self {
        PyField { field }
    }
}

impl From<PyField> for datatypes::Field {
    fn from(item: PyField) -> Self {
        item.field
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
