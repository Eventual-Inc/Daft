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
    ConnectTimeout(GenericError),
    ReadTimeout(GenericError),
    ByteStreamError(GenericError),
    SocketError(GenericError),
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
            DaftError::FileNotFound { source, .. }
            | DaftError::SocketError(source)
            | DaftError::External(source)
            | DaftError::ReadTimeout(source)
            | DaftError::ConnectTimeout(source)
            | DaftError::ByteStreamError(source) => Some(&**source),
            #[cfg(feature = "python")]
            DaftError::PyO3Error(pyerr) => Some(pyerr),
        }
    }
}

impl From<arrow2::error::Error> for DaftError {
    fn from(error: arrow2::error::Error) -> Self {
        match error {
            arrow2::error::Error::Io(_) => DaftError::ByteStreamError(error.into()),
            _ => DaftError::ArrowError(error.to_string()),
        }
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

impl From<regex::Error> for DaftError {
    fn from(error: regex::Error) -> Self {
        DaftError::ValueError(error.to_string())
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
            Self::ByteStreamError(e) => write!(f, "ByteStreamError: {}", e),
            Self::ConnectTimeout(e) => write!(f, "ConnectTimeout: {}", e),
            Self::ReadTimeout(e) => write!(f, "ReadTimeout: {}", e),
            Self::SocketError(e) => write!(f, "SocketError: {}", e),
        }
    }
}
