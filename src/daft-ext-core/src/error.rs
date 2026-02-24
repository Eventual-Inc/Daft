use std::fmt;

/// Result type for extension functions.
pub type DaftResult<T> = Result<T, DaftError>;

/// Errors produced by extension scalar functions.
#[derive(Debug)]
pub enum DaftError {
    /// Errors produced by the extension's type system.
    TypeError(String),
    /// Errors produced by the extension's runtime logic.
    RuntimeError(String),
}

impl fmt::Display for DaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimeError(msg) => write!(f, "RuntimeError: {msg}"),
            Self::TypeError(msg) => write!(f, "TypeError: {msg}"),
        }
    }
}

impl std::error::Error for DaftError {}
