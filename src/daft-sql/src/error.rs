use common_error::DaftError;
use snafu::Snafu;
use sqlparser::{parser::ParserError, tokenizer::TokenizerError};

#[derive(Debug, Snafu)]
pub enum PlannerError {
    #[snafu(display("Tokenization error: {source}"))]
    TokenizeError { source: TokenizerError },
    #[snafu(display("failed to parse sql: {source}"))]
    SQLParserError { source: ParserError },
    #[snafu(display("Parse error: {message}"))]
    ParseError { message: String },
    #[snafu(display("Invalid operation: {message}"))]
    InvalidOperation { message: String },
    #[snafu(display("Table not found: {message}"))]
    TableNotFound { message: String },
    #[snafu(display("Column {column_name} not found in {relation}"))]
    ColumnNotFound {
        column_name: String,
        relation: String,
    },
    #[snafu(display("Unsupported SQL: '{message}'"))]
    UnsupportedSQL { message: String },
    #[snafu(display("Daft error: {source}"))]
    DaftError { source: DaftError },
}

impl From<DaftError> for PlannerError {
    fn from(value: DaftError) -> Self {
        PlannerError::DaftError { source: value }
    }
}

impl From<TokenizerError> for PlannerError {
    fn from(value: TokenizerError) -> Self {
        PlannerError::TokenizeError { source: value }
    }
}

impl From<ParserError> for PlannerError {
    fn from(value: ParserError) -> Self {
        PlannerError::SQLParserError { source: value }
    }
}

impl PlannerError {
    pub fn column_not_found<A: Into<String>, B: Into<String>>(column_name: A, relation: B) -> Self {
        PlannerError::ColumnNotFound {
            column_name: column_name.into(),
            relation: relation.into(),
        }
    }

    pub fn table_not_found<S: Into<String>>(table_name: S) -> Self {
        PlannerError::TableNotFound {
            message: table_name.into(),
        }
    }

    pub fn unsupported_sql(sql: String) -> Self {
        PlannerError::UnsupportedSQL { message: sql }
    }

    pub fn invalid_operation<S: Into<String>>(message: S) -> Self {
        PlannerError::InvalidOperation {
            message: message.into(),
        }
    }
}

#[macro_export]
macro_rules! unsupported_sql_err {
    ($($arg:tt)*) => {
        return Err($crate::error::PlannerError::unsupported_sql(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! table_not_found_err {
    ($table_name:expr) => {
        return Err($crate::error::PlannerError::table_not_found($table_name))
    };
}

#[macro_export]
macro_rules! column_not_found_err {
    ($column_name:expr, $relation:expr) => {
        return Err($crate::error::PlannerError::column_not_found(
            $column_name,
            $relation,
        ))
    };
}

#[macro_export]
macro_rules! invalid_operation_err {
    ($($arg:tt)*) => {
        return Err($crate::error::PlannerError::invalid_operation(format!($($arg)*)))
    };
}

impl From<PlannerError> for DaftError {
    fn from(value: PlannerError) -> Self {
        if let PlannerError::DaftError { source } = value {
            source
        } else {
            DaftError::External(Box::new(value))
        }
    }
}

pub type SQLPlannerResult<T> = Result<T, PlannerError>;

#[cfg(feature = "python")]
use pyo3::{create_exception, exceptions::PyException, PyErr};

#[cfg(feature = "python")]
create_exception!(daft.exceptions, InvalidSQLException, PyException);

#[cfg(feature = "python")]
impl From<PlannerError> for PyErr {
    fn from(value: PlannerError) -> Self {
        InvalidSQLException::new_err(value.to_string())
    }
}
