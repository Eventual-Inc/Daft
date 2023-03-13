use pyo3::{exceptions::PyTypeError, exceptions::PyValueError, PyErr};

use crate::error::DaftError;

impl std::convert::From<DaftError> for PyErr {
    fn from(err: DaftError) -> PyErr {
        match err {
            DaftError::ExprResolveTypeError { .. } => PyTypeError::new_err(err.to_string()),
            DaftError::TypeError { .. } => PyTypeError::new_err(err.to_string()),
            _ => PyValueError::new_err(err.to_string()),
        }
    }
}
