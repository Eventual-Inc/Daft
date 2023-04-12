use pyo3::{exceptions::PyValueError, PyErr, Python};

use crate::error::DaftError;

impl std::convert::From<DaftError> for PyErr {
    fn from(err: DaftError) -> PyErr {
        match err {
            DaftError::PyO3Error(pyerr) => pyerr,
            _ => PyValueError::new_err(err.to_string()),
        }
    }
}

impl std::convert::From<PyErr> for DaftError {
    fn from(err: PyErr) -> DaftError {
        Python::with_gil(|py| {
            let traceback = err.traceback(py).and_then(|tb| match tb.format() {
                Ok(tb) => Some(tb),
                Err(_) => None,
            });
            match traceback {
                Some(traceback) => DaftError::ComputeError(format!(
                    "Error occurred when running Python: {err}\n\n{}",
                    traceback
                )),
                None => DaftError::ComputeError(format!(
                    "Error occurred when running Python (no traceback available): {err}"
                )),
            }
        })
    }
}
