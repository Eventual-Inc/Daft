use pyo3::{prelude::*, types::PyBytes, PyTypeInfo};
use serde::{Deserialize, Serialize};

use super::datatype::PyDataType;
use crate::datatypes;
use crate::impl_bincode_py_state_serialization;

#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyField {
    pub field: datatypes::Field,
}

#[pymethods]
impl PyField {
    #[staticmethod]
    pub fn create(name: &str, data_type: PyDataType) -> PyResult<Self> {
        Ok(datatypes::Field::new(name, data_type.dtype).into())
    }

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

impl_bincode_py_state_serialization!(PyField);

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
