use pyo3::create_exception;

use crate::DaftError;

impl From<pyo3::PyErr> for DaftError {
    fn from(error: pyo3::PyErr) -> Self {
        DaftError::PyO3Error(error)
    }
}

create_exception!(
    daft.daft,
    DaftCoreException,
    pyo3::exceptions::PyValueError,
    "DaftCore Base Exception"
);

impl std::convert::From<DaftError> for pyo3::PyErr {
    fn from(err: DaftError) -> pyo3::PyErr {
        use pyo3::exceptions::PyFileNotFoundError;

        match err {
            DaftError::PyO3Error(pyerr) => pyerr,
            DaftError::FileNotFound { path, source } => {
                PyFileNotFoundError::new_err(format!("File: {path} not found\n{source}"))
            }
            _ => DaftCoreException::new_err(err.to_string()),
        }
    }
}
