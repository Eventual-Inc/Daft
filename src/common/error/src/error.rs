use thiserror::Error;

pub type DaftResult<T> = std::result::Result<T, DaftError>;
pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum DaftError {
    #[error("DaftError::FieldNotFound {0}")]
    FieldNotFound(String),
    #[error("DaftError::SchemaMismatch {0}")]
    SchemaMismatch(String),
    #[error("DaftError::TypeError {0}")]
    TypeError(String),
    #[error("DaftError::ComputeError {0}")]
    ComputeError(String),
    #[error("DaftError::ArrowError {0}")]
    ArrowError(#[from] arrow2::error::Error),
    #[error("DaftError::ValueError {0}")]
    ValueError(String),
    #[cfg(feature = "python")]
    #[error("DaftError::PyO3Error {0}")]
    PyO3Error(#[from] pyo3::PyErr),
    #[error("DaftError::IoError {0}")]
    IoError(#[from] std::io::Error),
    #[error("DaftError::FileNotFound {path}: {source}")]
    FileNotFound { path: String, source: GenericError },
    #[error("DaftError::InternalError {0}")]
    InternalError(String),
    #[error("ConnectTimeout {0}")]
    ConnectTimeout(GenericError),
    #[error("ReadTimeout {0}")]
    ReadTimeout(GenericError),
    #[error("ByteStreamError {0}")]
    ByteStreamError(GenericError),
    #[error("SocketError {0}")]
    SocketError(GenericError),
    #[error("DaftError::External {0}")]
    External(GenericError),
    #[error("DaftError::SerdeJsonError {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("DaftError::FmtError {0}")]
    FmtError(#[from] std::fmt::Error),
    #[error("DaftError::RegexError {0}")]
    RegexError(#[from] regex::Error),
}
