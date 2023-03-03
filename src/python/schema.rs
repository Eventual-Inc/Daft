use pyo3::prelude::*;

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
        let fields: Vec<datatypes::Field> = self
            .schema
            .fields
            .values()
            .cloned()
            .chain(other.schema.fields.values().cloned())
            .collect();
        let new_schema = schema::Schema::new(fields);
        let new_pyschema = PySchema {
            schema: new_schema.into(),
        };
        Ok(new_pyschema)
    }

    pub fn eq(&self, other: &PySchema) -> PyResult<bool> {
        Ok(self.schema.fields.eq(&other.schema.fields))
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
    fn from(item: datatypes::Field) -> Self {
        PyField { field: item }
    }
}
