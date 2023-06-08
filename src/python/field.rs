use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyTuple},
};

use super::datatype::PyDataType;
use crate::datatypes::{self, DataType, Field};

#[pyclass]
#[derive(Clone)]
pub struct PyField {
    pub field: datatypes::Field,
}

#[pymethods]
impl PyField {
    #[new]
    #[pyo3(signature = (*args))]
    pub fn new(args: &PyTuple) -> PyResult<Self> {
        match args.len() {
            0 => Ok(Field::new("null", DataType::new_null()).into()),
            _ => Err(PyValueError::new_err(format!(
                "expected no arguments to make new PyDataType, got : {}",
                args.len()
            ))),
        }
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

    pub fn __setstate__(&mut self, py: Python, state: PyObject) -> PyResult<()> {
        match state.extract::<&PyBytes>(py) {
            Ok(s) => {
                self.field = bincode::deserialize(s.as_bytes()).unwrap();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn __getstate__(&self, py: Python) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, &bincode::serialize(&self.field).unwrap()).to_object(py))
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
