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

// Blanket impl: any `std::error::Error` can be converted to a `DaftError` via `?`.
// Note: DaftError intentionally does NOT implement `std::error::Error` to avoid
// coherence conflicts with this blanket impl (`From<T> for T`).
impl<E: std::error::Error> From<E> for DaftError {
    fn from(e: E) -> Self {
        Self::RuntimeError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_error_display() {
        let err = DaftError::TypeError("expected Int32".into());
        assert_eq!(err.to_string(), "TypeError: expected Int32");
    }

    #[test]
    fn runtime_error_display() {
        let err = DaftError::RuntimeError("division by zero".into());
        assert_eq!(err.to_string(), "RuntimeError: division by zero");
    }

    #[test]
    fn from_std_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let daft_err: DaftError = io_err.into();
        assert!(daft_err.to_string().contains("file missing"));
    }

    #[test]
    fn error_debug_format() {
        let err = DaftError::TypeError("bad".into());
        let debug = format!("{err:?}");
        assert!(debug.contains("TypeError"));
        assert!(debug.contains("bad"));
    }
}
