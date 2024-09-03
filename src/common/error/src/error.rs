use thiserror::Error;

pub type DaftResult<T> = std::result::Result<T, DaftError>;
pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum DaftError {
    #[error("{0:?}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("{0}")]
    ComputeError(String),
    #[error("{0}")]
    FieldNotFound(String),
    #[error("{0}")]
    SchemaMismatch(String),
    #[error("{0}")]
    TypeError(String),
    #[error("{0:?}")]
    ArrowError(#[from] arrow2::error::Error),
    #[error("{0}")]
    ValueError(String),
    #[cfg(feature = "python")]
    #[error("{0:?}")]
    PyO3Error(#[from] pyo3::PyErr),
    #[error("{0:?}")]
    IoError(#[from] std::io::Error),
    #[error("{0:?}")]
    FmtError(#[from] std::fmt::Error),
    #[error("DaftError::FileNotFound {path}: {source}")]
    FileNotFound { path: String, source: GenericError },
    #[error("{0}")]
    InternalError(String),
    #[error("{0:?}")]
    ConnectTimeout(GenericError),
    #[error("{0:?}")]
    ReadTimeout(GenericError),
    #[error("{0:?}")]
    ByteStreamError(GenericError),
    #[error("{0:?}")]
    SocketError(GenericError),
    #[error("{0:?}")]
    RegexError(#[from] regex::Error),
    #[error("{0:?}")]
    External(GenericError),
}
