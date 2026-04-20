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

    /// Append context (e.g. source file path) to the error message,
    /// preserving the original variant for correct Python exception mapping.
    pub fn with_context(self, context: impl std::fmt::Display) -> Self {
        match self {
            Self::FileNotFound { .. } => self,
            #[cfg(feature = "python")]
            Self::PyO3Error(_) => self,
            Self::AmbiguousReference(msg) => {
                Self::AmbiguousReference(format!("{msg}\n\nContext: {context}"))
            }
            Self::FieldNotFound(msg) => Self::FieldNotFound(format!("{msg}\n\nContext: {context}")),
            Self::SchemaMismatch(msg) => {
                Self::SchemaMismatch(format!("{msg}\n\nContext: {context}"))
            }
            Self::TypeError(msg) => Self::TypeError(format!("{msg}\n\nContext: {context}")),
            Self::ComputeError(msg) => Self::ComputeError(format!("{msg}\n\nContext: {context}")),
            Self::ParquetError(msg) => Self::ParquetError(format!("{msg}\n\nContext: {context}")),
            Self::ValueError(msg) => Self::ValueError(format!("{msg}\n\nContext: {context}")),
            Self::InternalError(msg) => Self::InternalError(format!("{msg}\n\nContext: {context}")),
            Self::NotImplemented(msg) => {
                Self::NotImplemented(format!("{msg}\n\nContext: {context}"))
            }
            Self::CatalogError(msg) => Self::CatalogError(format!("{msg}\n\nContext: {context}")),
            Self::InvalidArgumentError(msg) => {
                Self::InvalidArgumentError(format!("{msg}\n\nContext: {context}"))
            }
            Self::ConnectTimeout(inner) => {
                Self::ConnectTimeout(format!("{inner}\n\nContext: {context}").into())
            }
            Self::ReadTimeout(inner) => {
                Self::ReadTimeout(format!("{inner}\n\nContext: {context}").into())
            }
            Self::ByteStreamError(inner) => {
                Self::ByteStreamError(format!("{inner}\n\nContext: {context}").into())
            }
            Self::SocketError(inner) => {
                Self::SocketError(format!("{inner}\n\nContext: {context}").into())
            }
            Self::ThrottledIo(inner) => {
                Self::ThrottledIo(format!("{inner}\n\nContext: {context}").into())
            }
            Self::MiscTransient(inner) => {
                Self::MiscTransient(format!("{inner}\n\nContext: {context}").into())
            }
            Self::External(inner) => {
                Self::External(format!("{inner}\n\nContext: {context}").into())
            }
            // JoinError carries structured data (is_cancelled) used by shutdown logic;
            // converting it to External would break cancellation detection.
            Self::JoinError(_) => self,
            Self::IoError(inner) => Self::External(format!("{inner}\n\nContext: {context}").into()),
            Self::ArrowRsError(inner) => {
                Self::External(format!("{inner}\n\nContext: {context}").into())
            }
            Self::SerdeJsonError(inner) => {
                Self::External(format!("{inner}\n\nContext: {context}").into())
            }
            Self::FmtError(inner) => {
                Self::External(format!("{inner}\n\nContext: {context}").into())
            }
            Self::RegexError(inner) => {
                Self::External(format!("{inner}\n\nContext: {context}").into())
            }
            Self::FromUtf8Error(inner) => {
                Self::External(format!("{inner}\n\nContext: {context}").into())
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_context_string_variant() {
        let err = DaftError::ValueError("original error".to_string());
        let err_with_ctx = err.with_context("reading file.json");

        match err_with_ctx {
            DaftError::ValueError(msg) => {
                assert_eq!(msg, "original error\n\nContext: reading file.json");
            }
            _ => panic!("Variant should not change"),
        }
    }

    #[test]
    fn test_with_context_generic_error_variant() {
        let err = DaftError::External("external error".into());
        let err_with_ctx = err.with_context("reading file.json");

        match err_with_ctx {
            DaftError::External(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "external error\n\nContext: reading file.json"
                );
            }
            _ => panic!("Variant should not change"),
        }
    }

    #[test]
    fn test_with_context_file_not_found_passthrough() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let err = DaftError::FileNotFound {
            path: "file.json".to_string(),
            source: Box::new(io_err),
        };
        let err_with_ctx = err.with_context("reading file.json");

        match err_with_ctx {
            DaftError::FileNotFound { path, .. } => {
                assert_eq!(path, "file.json");
            }
            _ => panic!("Variant should not change"),
        }
    }

    #[test]
    fn test_with_context_from_variant_converts_to_external() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let err = DaftError::IoError(io_err);
        let err_with_ctx = err.with_context("reading file.json");

        match err_with_ctx {
            DaftError::External(msg) => {
                assert!(
                    msg.to_string()
                        .contains("not found\n\nContext: reading file.json")
                );
            }
            _ => panic!("Variant should change to External"),
        }
    }

    #[test]
    fn test_with_context_join_error_passthrough() {
        let handle = tokio::runtime::Runtime::new()
            .unwrap()
            .spawn(async { panic!("test") });
        let join_err = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle)
            .unwrap_err();
        let err = DaftError::JoinError(join_err);
        let err_with_ctx = err.with_context("reading file.json");

        assert!(matches!(err_with_ctx, DaftError::JoinError(_)));
    }
}
