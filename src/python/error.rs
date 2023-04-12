use pyo3::{exceptions::PyValueError, PyErr};

use crate::error::DaftError;

impl std::convert::From<DaftError> for PyErr {
    fn from(err: DaftError) -> PyErr {
        match err {
            DaftError::PyO3Error(pyerr) => pyerr,
            _ => PyValueError::new_err(err.to_string()),
        }
    }
}
