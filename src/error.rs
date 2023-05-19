use std::{
    fmt::{Display, Formatter, Result},
    io,
};

#[derive(Debug)]
pub enum DaftError {
    NotFound(String),
    SchemaMismatch(String),
    TypeError(String),
    ComputeError(String),
    ArrowError(String),
    ValueError(String),
    #[cfg(feature = "python")]
    PyO3Error(pyo3::PyErr),
    IoError(io::Error),
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

pub type DaftResult<T> = std::result::Result<T, DaftError>;

impl Display for DaftError {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            Self::NotFound(s) => write!(f, "DaftError::NotFound {s}"),
            Self::SchemaMismatch(s) => write!(f, "DaftError::SchemaMismatch {s}"),
            Self::TypeError(s) => write!(f, "DaftError::TypeError {s}"),
            Self::ComputeError(s) => write!(f, "DaftError::ComputeError {s}"),
            Self::ArrowError(s) => write!(f, "DaftError::ArrowError {s}"),
            Self::ValueError(s) => write!(f, "DaftError::ValueError {s}"),
            #[cfg(feature = "python")]
            Self::PyO3Error(e) => write!(f, "DaftError::PyO3Error {e}"),
            Self::IoError(e) => write!(f, "DaftError::IoError {e}"),
        }
    }
}
