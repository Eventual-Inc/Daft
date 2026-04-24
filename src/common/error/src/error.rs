use thiserror::Error;

pub type DaftResult<T> = std::result::Result<T, DaftError>;
pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum DaftError {
    #[error("DaftError::AmbiguousReference {0}")]
    AmbiguousReference(String),
    #[error("DaftError::FieldNotFound {0}")]
    FieldNotFound(String),
    #[error("DaftError::SchemaMismatch {0}")]
    SchemaMismatch(String),
    #[error("DaftError::TypeError {0}")]
    TypeError(String),
    #[error("DaftError::ComputeError {0}")]
    ComputeError(String),
    #[error("DaftError::ArrowRsError {0}")]
    ArrowRsError(#[from] arrow_schema::ArrowError),
    // TODO(desmond): We can't currently implement this as a From<parquet::errors::ParquetError>
    // because this results in infinite nesting of types in `fixed_size_binary_op` in arithmetic.rs.
    #[error("DaftError::ParquetError {0}")]
    ParquetError(String),
    /// Raised when a file is identified as corrupt or unreadable due to format/integrity
    /// failures (e.g. bad magic bytes, truncated footer, bad encoding, wrong field counts).
    /// Used by `is_parquet_corrupt` and `is_csv_corrupt` to identify files that should be
    /// skipped when `ignore_corrupt_files` is enabled.
    /// General operation errors (write failures, schema mismatches, etc.) are NOT routed
    /// here — they use format-specific variants or `External`.
    #[error("DaftError::CorruptFile {0}")]
    CorruptFile(String),
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
    #[error("ThrottledIo {0}")]
    ThrottledIo(GenericError),
    #[error("MiscTransient {0}")]
    MiscTransient(GenericError),
    #[error("DaftError::External {0}")]
    External(GenericError),
    #[error("DaftError::SerdeJsonError {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("DaftError::FmtError {0}")]
    FmtError(#[from] std::fmt::Error),
    #[error("DaftError::RegexError {0}")]
    RegexError(#[from] regex::Error),
    #[error("DaftError::FromUtf8Error {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Not Yet Implemented: {0}")]
    NotImplemented(String),
    #[error("DaftError::CatalogError {0}")]
    CatalogError(String),
    #[error("DaftError::JoinError {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("DaftError::InvalidArgumentError {0}")]
    InvalidArgumentError(String),
}

impl DaftError {
    pub fn not_implemented<T: std::fmt::Display>(msg: T) -> Self {
        Self::NotImplemented(msg.to_string())
    }
    pub fn type_error<T: std::fmt::Display>(msg: T) -> Self {
        Self::TypeError(msg.to_string())
    }
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err($crate::DaftError::ComputeError($msg.to_string()));
        }
    };
    ($cond:expr, $variant:ident: $($msg:tt)*) => {
        if !$cond {
            return Err($crate::DaftError::$variant(format!($($msg)*)));
        }
    };
}

#[macro_export]
macro_rules! value_err {
    ($($arg:tt)*) => {
        return Err(common_error::DaftError::ValueError(format!($($arg)*)))
    };
}

#[cfg(feature = "python")]
impl<'py> From<pyo3::pyclass::PyClassGuardError<'_, 'py>> for DaftError {
    fn from(error: pyo3::pyclass::PyClassGuardError<'_, 'py>) -> Self {
        Self::PyO3Error(error.into())
    }
}
