use pyo3::{
    exceptions::{PyFileNotFoundError, PyValueError},
    PyErr,
};

use crate::error::DaftError;

impl std::convert::From<DaftError> for PyErr {
    fn from(err: DaftError) -> PyErr {
        match err {
            DaftError::PyO3Error(pyerr) => pyerr,
            DaftError::FileNotFound { path, source } => {
                PyFileNotFoundError::new_err(format!("File: {path} not found\n{source}"))
            }
            _ => PyValueError::new_err(err.to_string()),
        }
    }
}
