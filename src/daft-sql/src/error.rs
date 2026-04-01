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
    #[snafu(display("{message}"))]
    CaretError { message: String },
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

    pub fn caret_error(message: String) -> Self {
        Self::CaretError { message }
    }
}

/// Format a SQL error message with source context and a caret (^) pointer.
pub fn format_sql_error_with_caret(sql: &str, reason: &str, line: u64, column: u64) -> String {
    if line == 0 {
        return reason.to_string();
    }
    let lines: Vec<&str> = sql.lines().collect();
    let line_idx = (line as usize).saturating_sub(1);
    if line_idx >= lines.len() {
        return reason.to_string();
    }
    let offending_line = lines[line_idx];
    let prefix = format!("LINE {}: ", line);
    let col = if column == 0 {
        0usize
    } else {
        (column as usize - 1).min(offending_line.len())
    };
    let caret_padding = " ".repeat(prefix.len() + col);
    format!(
        "SQL error at Line: {line}, Column: {column}\n{prefix}{offending_line}\n{caret_padding}^\nREASON: {reason}"
    )
}

/// Extract line/column from a sqlparser error message string and format with caret.
///
/// sqlparser embeds location as ` at Line: X, Column: Y` at the end of its error strings.
/// This function parses that suffix, strips it to get the reason, then calls `format_sql_error_with_caret`.
/// If no location is found (e.g. EOF errors), computes the position from the SQL text.
pub fn format_sql_error_from_message(sql: &str, error_msg: &str) -> String {
    if let Some((reason, line, column)) = extract_location_from_message(error_msg) {
        format_sql_error_with_caret(sql, reason, line, column)
    } else {
        // For EOF errors, sqlparser uses Span::empty() (line=0, col=0) so no location
        // suffix is appended. Compute the EOF position from the SQL text itself.
        let (eof_line, eof_col) = eof_position(sql);
        format_sql_error_with_caret(sql, error_msg, eof_line, eof_col)
    }
}

/// Compute the 1-indexed line and column of the position just past the end of the SQL text.
fn eof_position(sql: &str) -> (u64, u64) {
    let lines: Vec<&str> = sql.lines().collect();
    if lines.is_empty() {
        return (0, 0);
    }
    let last_line = lines.len();
    let last_col = lines[last_line - 1].len() + 1;
    (last_line as u64, last_col as u64)
}

/// Try to extract ` at Line: X, Column: Y` from the end of an error message.
/// Returns (reason_without_suffix, line, column) if found.
fn extract_location_from_message(msg: &str) -> Option<(&str, u64, u64)> {
    let marker = " at Line: ";
    let marker_pos = msg.rfind(marker)?;
    let rest = &msg[marker_pos + marker.len()..];
    let comma_pos = rest.find(", Column: ")?;
    let line: u64 = rest[..comma_pos].parse().ok()?;
    let column: u64 = rest[comma_pos + ", Column: ".len()..].parse().ok()?;
    if line == 0 {
        return None;
    }
    let reason = &msg[..marker_pos];
    Some((reason, line, column))
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
