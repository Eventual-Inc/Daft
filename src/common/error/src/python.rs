use pyo3::{exceptions::PyFileNotFoundError, import_exception};

use crate::{DaftError, format::format_error_for_user};

import_exception!(daft.exceptions, DaftCoreException);
import_exception!(daft.exceptions, DaftTypeError);
import_exception!(daft.exceptions, ConnectTimeoutError);
import_exception!(daft.exceptions, ReadTimeoutError);
import_exception!(daft.exceptions, ByteStreamError);
import_exception!(daft.exceptions, SocketError);
import_exception!(daft.exceptions, ThrottleError);
import_exception!(daft.exceptions, MiscTransientError);

impl std::convert::From<DaftError> for pyo3::PyErr {
    fn from(err: DaftError) -> Self {
        match err {
            DaftError::PyO3Error(pyerr) => pyerr,
            DaftError::TypeError(msg) => DaftTypeError::new_err(msg),
            other => {
                let formatted = format_error_for_user(&other);
                match other {
                    DaftError::FileNotFound { .. } => PyFileNotFoundError::new_err(formatted),
                    DaftError::ConnectTimeout(_) => ConnectTimeoutError::new_err(formatted),
                    DaftError::ReadTimeout(_) => ReadTimeoutError::new_err(formatted),
                    DaftError::ByteStreamError(_) => ByteStreamError::new_err(formatted),
                    DaftError::SocketError(_) => SocketError::new_err(formatted),
                    DaftError::ThrottledIo(_) => ThrottleError::new_err(formatted),
                    DaftError::MiscTransient(_) => MiscTransientError::new_err(formatted),
                    _ => DaftCoreException::new_err(formatted),
                }
            }
        }
    }
}
