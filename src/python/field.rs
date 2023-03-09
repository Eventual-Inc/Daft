use pyo3::prelude::*;

use super::datatype::PyDataType;
use crate::datatypes;

#[pyclass]
pub struct PyField {
    pub field: datatypes::Field,
}

#[pymethods]
impl PyField {
    pub fn name(&self) -> PyResult<String> {
        Ok(self.field.name.clone())
    }

    pub fn dtype(&self) -> PyResult<PyDataType> {
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
