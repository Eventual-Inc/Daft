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
    fn error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(DaftError::TypeError("test".into()));
        assert!(err.to_string().contains("TypeError"));
    }

    #[test]
    fn error_debug_format() {
        let err = DaftError::TypeError("bad".into());
        let debug = format!("{err:?}");
        assert!(debug.contains("TypeError"));
        assert!(debug.contains("bad"));
    }
}
