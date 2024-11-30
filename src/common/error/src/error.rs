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
    ArrowError(arrow2::error::Error),
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
}

impl DaftError {
    pub fn not_implemented<T: std::fmt::Display>(msg: T) -> Self {
        Self::NotImplemented(msg.to_string())
    }
}

impl From<arrow2::error::Error> for DaftError {
    fn from(error: arrow2::error::Error) -> Self {
        match error {
            arrow2::error::Error::Io(_) => Self::ByteStreamError(error.into()),
            _ => Self::ArrowError(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;

    #[test]
    fn test_arrow_io_error_conversion() {
        // Ensure that arrow2 IO errors get converted into transient Byte Stream errors.
        let error_message = "IO error occurred";
        let arrow_io_error =
            arrow2::error::Error::Io(std::io::Error::new(ErrorKind::Other, error_message));
        let daft_error: DaftError = arrow_io_error.into();
        match daft_error {
            DaftError::ByteStreamError(e) => {
                assert_eq!(e.to_string(), format!("Io error: {error_message}"));
            }
            _ => panic!("Expected ByteStreamError"),
        }
    }

    #[test]
    fn test_parquet_io_error_conversion() {
        // Ensure that parquet2 IO errors get converted into transient Byte Stream errors.
        let error_message = "IO error occurred";
        let parquet_io_error =
            parquet2::error::Error::IoError(std::io::Error::new(ErrorKind::Other, error_message));
        let arrow_error: arrow2::error::Error = parquet_io_error.into();
        //let arrow_error = arrow2::error::Error::from(parquet_io_error);
        let daft_error: DaftError = arrow_error.into();
        match daft_error {
            DaftError::ByteStreamError(e) => {
                assert_eq!(e.to_string(), format!("Io error: {error_message}"));
            }
            _ => panic!("Expected ByteStreamError"),
        }
    }
}
