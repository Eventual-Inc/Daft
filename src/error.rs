use std::result;

#[derive(Debug)]
pub enum DaftError {
    SchemaMismatch(String),
}

pub type DaftResult<T> = std::result::Result<T, DaftError>;
