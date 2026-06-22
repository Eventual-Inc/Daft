use common_error::DaftError;
use daft_catalog::error::CatalogError;
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
    #[snafu(display("Invalid argument ({message}) for function '{function}'"))]
    InvalidFunctionArgument { message: String, function: String },
    #[snafu(display("Table not found: {message}"))]
    TableNotFound { message: String },
    #[snafu(display("Column {column_name} not found in {relation}"))]
    ColumnNotFound {
        column_name: String,
        relation: String,
    },
    #[snafu(display("Unsupported SQL: '{message}'"))]
    UnsupportedSQL { message: String },
    #[snafu(display(
        "SQL error at Line: {line}, Column: {column}\nLINE {line}: {line_content}\n{caret_padding}^\nREASON: {reason}"
    ))]
    CaretError {
        line: u64,
        column: u64,
        line_content: String,
        reason: String,
        caret_padding: String,
    },
    #[snafu(display("Daft error: {source}"))]
    DaftError { source: DaftError },
}

impl From<DaftError> for PlannerError {
    fn from(value: DaftError) -> Self {
        Self::DaftError { source: value }
    }
}

impl From<TokenizerError> for PlannerError {
    fn from(value: TokenizerError) -> Self {
        Self::TokenizeError { source: value }
    }
}

impl From<ParserError> for PlannerError {
    fn from(value: ParserError) -> Self {
        Self::SQLParserError { source: value }
    }
}

impl From<CatalogError> for PlannerError {
    fn from(value: CatalogError) -> Self {
        match value {
            CatalogError::TableNotFound {
                catalog_name,
                table_id,
            } => Self::table_not_found(format!("{}.{}", catalog_name, table_id)),
            _ => Self::invalid_operation(value.to_string()),
        }
    }
}

impl PlannerError {
    pub fn column_not_found<A: Into<String>, B: Into<String>>(column_name: A, relation: B) -> Self {
        Self::ColumnNotFound {
            column_name: column_name.into(),
            relation: relation.into(),
        }
    }

    pub fn table_not_found<S: Into<String>>(table_name: S) -> Self {
        Self::TableNotFound {
            message: table_name.into(),
        }
    }

    #[must_use]
    pub fn unsupported_sql(sql: String) -> Self {
        Self::UnsupportedSQL { message: sql }
    }

    pub fn invalid_operation<S: Into<String>>(message: S) -> Self {
        Self::InvalidOperation {
            message: message.into(),
        }
    }

    pub fn invalid_argument<S: Into<String>, F: Into<String>>(arg: S, function: F) -> Self {
        Self::InvalidFunctionArgument {
            message: arg.into(),
            function: function.into(),
        }
    }

    pub fn caret_error(reason: String, sql: &str, line: u64, column: u64) -> Self {
        let lines: Vec<&str> = sql.lines().collect();
        let line_idx = if line == 0 {
            0
        } else {
            (line as usize).saturating_sub(1)
        };
        let line_content = if line_idx < lines.len() {
            lines[line_idx].to_string()
        } else {
            String::new()
        };
        let col = if column == 0 {
            0usize
        } else {
            (column as usize - 1).min(line_content.len())
        };
        let prefix_len = if line == 0 {
            0
        } else {
            format!("LINE {}: ", line).len()
        };
        let caret_padding = " ".repeat(prefix_len + col);

        Self::CaretError {
            line,
            column,
            line_content,
            reason,
            caret_padding,
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
#[macro_export]
macro_rules! ensure {
    ($condition:expr, $($arg:tt)*) => {
        if !$condition {
            return Err($crate::error::PlannerError::invalid_operation(format!($($arg)*)))
        }
    };
}

impl From<PlannerError> for DaftError {
    fn from(value: PlannerError) -> Self {
        if let PlannerError::DaftError { source } = value {
            source
        } else {
            Self::External(Box::new(value))
        }
    }
}

pub type SQLPlannerResult<T> = Result<T, PlannerError>;

#[cfg(feature = "python")]
use pyo3::{PyErr, create_exception, exceptions::PyException};

#[cfg(feature = "python")]
create_exception!(daft.exceptions, InvalidSQLException, PyException);

#[cfg(feature = "python")]
impl From<PlannerError> for PyErr {
    fn from(value: PlannerError) -> Self {
        InvalidSQLException::new_err(value.to_string())
    }
}
