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
}

#[pymethods]
impl PyField {
    pub fn name(&self) -> PyResult<String> {
        Ok(self.field.name.clone())
    }

    pub fn dtype(&self) -> PyResult<datatype::PyDataType> {
        Ok(self.field.dtype.clone().into())
    }
}

impl From<datatypes::Field> for PyField {
    fn from(item: datatypes::Field) -> Self {
        PyField { field: item }
    }
}
