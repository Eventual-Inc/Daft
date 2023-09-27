use std::{
    fmt::{Display, Formatter, Result},
    io,
};

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub enum DaftError {
    FieldNotFound(String),
    SchemaMismatch(String),
    TypeError(String),
    ComputeError(String),
    ArrowError(String),
    ValueError(String),
    #[cfg(feature = "python")]
    PyO3Error(pyo3::PyErr),
    IoError(io::Error),
    FileNotFound {
        path: String,
        source: GenericError,
    },
    InternalError(String),
    External(GenericError),
}

impl std::error::Error for DaftError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DaftError::FieldNotFound(_)
            | DaftError::SchemaMismatch(_)
            | DaftError::TypeError(_)
            | DaftError::ComputeError(_)
            | DaftError::ArrowError(_)
            | DaftError::ValueError(_)
            | DaftError::InternalError(_) => None,
            DaftError::IoError(io_error) => Some(io_error),
            DaftError::FileNotFound { source, .. } | DaftError::External(source) => Some(&**source),
            #[cfg(feature = "python")]
            DaftError::PyO3Error(pyerr) => Some(pyerr),
        }
    }
}

impl From<arrow2::error::Error> for DaftError {
    fn from(error: arrow2::error::Error) -> Self {
        DaftError::ArrowError(error.to_string())
    }
}

#[cfg(feature = "python")]
impl From<pyo3::PyErr> for DaftError {
    fn from(error: pyo3::PyErr) -> Self {
        DaftError::PyO3Error(error)
    }
}

impl From<serde_json::Error> for DaftError {
    fn from(error: serde_json::Error) -> Self {
        DaftError::IoError(error.into())
    }
}

impl From<io::Error> for DaftError {
    fn from(error: io::Error) -> Self {
        DaftError::IoError(error)
    }
}
#[cfg(feature = "python")]
impl std::convert::From<DaftError> for pyo3::PyErr {
    fn from(err: DaftError) -> pyo3::PyErr {
        use pyo3::exceptions::{PyFileNotFoundError, PyValueError};
        match err {
            DaftError::PyO3Error(pyerr) => pyerr,
            DaftError::FileNotFound { path, source } => {
                PyFileNotFoundError::new_err(format!("File: {path} not found\n{source}"))
            }
            _ => PyValueError::new_err(err.to_string()),
        }
    }
}

impl From<std::fmt::Error> for DaftError {
    fn from(error: std::fmt::Error) -> Self {
        DaftError::ComputeError(error.to_string())
    }
}

pub type DaftResult<T> = std::result::Result<T, DaftError>;

impl Display for DaftError {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            Self::FieldNotFound(s) => write!(f, "DaftError::FieldNotFound {s}"),
            Self::SchemaMismatch(s) => write!(f, "DaftError::SchemaMismatch {s}"),
            Self::TypeError(s) => write!(f, "DaftError::TypeError {s}"),
            Self::ComputeError(s) => write!(f, "DaftError::ComputeError {s}"),
            Self::ArrowError(s) => write!(f, "DaftError::ArrowError {s}"),
            Self::ValueError(s) => write!(f, "DaftError::ValueError {s}"),
            Self::InternalError(s) => write!(f, "DaftError::InternalError {s}"),
            #[cfg(feature = "python")]
            Self::PyO3Error(e) => write!(f, "DaftError::PyO3Error {e}"),
            Self::IoError(e) => write!(f, "DaftError::IoError {e}"),
            Self::External(e) => write!(f, "DaftError::External {}", e),
            Self::FileNotFound { path, source } => {
                write!(f, "DaftError::FileNotFound {path}: {source}")
            }
        }
    }
}
