use std::result;

#[derive(Debug)]
pub enum DaftError {
    NotFound(String),
    SchemaMismatch(String),
    TypeError(String),
}

pub type DaftResult<T> = std::result::Result<T, DaftError>;
