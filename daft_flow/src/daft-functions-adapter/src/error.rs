use thiserror::Error;

/// Custom error type for daft-functions-adapter
#[derive(Debug, Error)]
pub enum FunctionsAdapterError {
    #[error("Type mismatch: expected {expected}, but got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Invalid series data: {message}")]
    InvalidSeriesData { message: String },

    #[error("Iterator error: {message}")]
    IteratorError { message: String },

    #[error("Daft error: {source}")]
    DaftError {
        #[from]
        source: common_error::DaftError,
    },
}

/// Result type for daft-functions-adapter operations
pub type FunctionsAdapterResult<T> = Result<T, FunctionsAdapterError>;
