use std::fmt::{Display, Formatter, Result};

use crate::{datatypes::Field, dsl::Expr};
use std::sync::Arc;

#[derive(Debug)]
pub enum DaftError {
    NotFound(String),
    SchemaMismatch(String),
    TypeError(String),
    ComputeError(String),
    ArrowError(String),
    ValueError(String),
    // ExprResolveTypeError: Typing error when resolving expressions against schemas
    //
    // This variant has custom Display logic for presenting a more user-friendly error message which shows
    // exactly which operation, arguments and dtypes of those arguments caused the issue.
    ExprResolveTypeError {
        expectation: String,
        expr_repr: String,
        child_fields_to_expr: Vec<(Field, Arc<Expr>)>,
    },
}

impl From<arrow2::error::Error> for DaftError {
    fn from(error: arrow2::error::Error) -> Self {
        DaftError::ArrowError(error.to_string())
    }
}

pub type DaftResult<T> = std::result::Result<T, DaftError>;

impl Display for DaftError {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        // Override this method if the error propagated should have custom display for better user-level error messages
        match self {
            Self::ExprResolveTypeError {
                expectation,
                expr_repr,
                child_fields_to_expr,
            } => {
                writeln!(
                    f,
                    "Expects {expectation}, but failed type resolution: {expr_repr}",
                )?;
                writeln!(f, "where expression arguments are:")?;
                for (field, expr) in child_fields_to_expr.iter() {
                    writeln!(
                        f,
                        "  `{}` resolves to {}: {}",
                        field.name, field.dtype, expr
                    )?;
                }
                Ok(())
            }
            _ => write!(f, "{self:?}"),
        }
    }
}
