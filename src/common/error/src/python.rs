use pyo3::import_exception;

use crate::DaftError;

impl From<pyo3::PyErr> for DaftError {
    fn from(error: pyo3::PyErr) -> Self {
        DaftError::PyO3Error(error)
    }
}

import_exception!(daft.exceptions, DaftCoreException);
import_exception!(daft.exceptions, DaftTypeError);
import_exception!(daft.exceptions, ConnectTimeoutError);
import_exception!(daft.exceptions, ReadTimeoutError);
import_exception!(daft.exceptions, ByteStreamError);
import_exception!(daft.exceptions, SocketError);

impl std::convert::From<DaftError> for pyo3::PyErr {
    fn from(err: DaftError) -> pyo3::PyErr {
        use pyo3::exceptions::PyFileNotFoundError;

        match err {
            DaftError::PyO3Error(pyerr) => pyerr,
            DaftError::FileNotFound { path, source } => {
                PyFileNotFoundError::new_err(format!("File: {path} not found\n{source}"))
            }
            DaftError::TypeError(err) => DaftTypeError::new_err(err),
            DaftError::ConnectTimeout(err) => ConnectTimeoutError::new_err(err.to_string()),
            DaftError::ReadTimeout(err) => ReadTimeoutError::new_err(err.to_string()),
            DaftError::ByteStreamError(err) => ByteStreamError::new_err(err.to_string()),
            DaftError::SocketError(err) => SocketError::new_err(err.to_string()),
            _ => DaftCoreException::new_err(err.to_string()),
        }
    }
}
