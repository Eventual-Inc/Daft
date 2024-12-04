use common_py_serde::impl_bincode_py_state_serialization;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use super::datatype::PyDataType;
use crate::field::Field;

#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyField {
    pub field: Field,
}

#[pymethods]
impl PyField {
    #[staticmethod]
    pub fn create(name: &str, data_type: PyDataType) -> PyResult<Self> {
        Ok(Field::new(name, data_type.dtype).into())
    }

    pub fn name(&self) -> PyResult<String> {
        Ok(self.field.name.clone())
    }

    pub fn dtype(&self) -> PyResult<PyDataType> {
        Ok(self.field.dtype.clone().into())
    }

    pub fn eq(&self, other: &Self) -> PyResult<bool> {
        Ok(self.field.eq(&other.field))
    }
}

impl_bincode_py_state_serialization!(PyField);

impl From<Field> for PyField {
    fn from(field: Field) -> Self {
        Self { field }
    }
}

impl From<PyField> for Field {
    fn from(item: PyField) -> Self {
        item.field
    }
}
