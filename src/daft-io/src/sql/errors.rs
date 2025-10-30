use std::fmt;

#[derive(Debug)]
pub enum SqlError {
    Connection(String),
    Database(String),
    TypeConversion(String),
    Schema(String),
    Io(String),
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlError::Connection(msg) => write!(f, "Connection error: {}", msg),
            SqlError::Database(msg) => write!(f, "Database error: {}", msg),
            SqlError::TypeConversion(msg) => write!(f, "Type conversion error: {}", msg),
            SqlError::Schema(msg) => write!(f, "Schema error: {}", msg),
            SqlError::Io(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl std::error::Error for SqlError {}

pub type SqlResult<T> = Result<T, SqlError>;
