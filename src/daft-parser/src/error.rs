use std::ops::Range;

use crate::lexer::Token;

/// Owned so it can outlive the input; lalrpop's error borrows tokens from it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message} at {}..{}", .span.start, .span.end)]
pub struct ParseError {
    pub message: String,
    pub span: Range<usize>,
}

impl ParseError {
    pub fn new(message: impl Into<String>, span: Range<usize>) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }

    pub(crate) fn from_lalrpop(
        err: lalrpop_util::ParseError<usize, Token<'_>, Self>,
        input: &str,
    ) -> Self {
        use lalrpop_util::ParseError::*;
        match err {
            InvalidToken { location } => Self::new("invalid token", location..location),
            UnrecognizedEof { location, expected } => Self::new(
                format!(
                    "unexpected end of input, expected one of {}",
                    expected.join(", ")
                ),
                location..location,
            ),
            UnrecognizedToken {
                token: (start, _, end),
                expected,
            } => Self::new(
                format!(
                    "unexpected `{}`, expected one of {}",
                    &input[start..end],
                    expected.join(", ")
                ),
                start..end,
            ),
            ExtraToken {
                token: (start, _, end),
            } => Self::new(format!("unexpected `{}`", &input[start..end]), start..end),
            User { error } => error,
        }
    }
}
