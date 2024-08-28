use daft_dsl::{Expr, ExprRef, LiteralValue};

use super::SQLModule;
use crate::{
    error::SQLPlannerResult,
    functions::{SQLFunction, SQLFunctions},
    unsupported_sql_err,
};

pub struct SQLModuleImage;

impl SQLModule for SQLModuleImage {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("image_decode", SQLImageDecode {});
    }
}

pub struct SQLImageDecode;

impl SQLFunction for SQLImageDecode {
    fn to_expr(&self, inputs: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
        use daft_functions::image::decode;
        match inputs {
            [input] => Ok(decode(input.clone(), true, None)),
            [input, mode] => {
                let mode = match mode.as_ref() {
                    Expr::Literal(LiteralValue::Utf8(s)) => Some(s.parse()?),
                    _ => unsupported_sql_err!("Expected mode to be a string"),
                };
                Ok(decode(input.clone(), true, mode))
            }
            [input, raise_on_error, mode] => {
                let raise_on_error = match raise_on_error.as_ref() {
                    Expr::Literal(LiteralValue::Boolean(b)) => *b,
                    _ => unsupported_sql_err!("Expected raise_on_error to be a boolean"),
                };
                let mode = match mode.as_ref() {
                    Expr::Literal(LiteralValue::Utf8(s)) => Some(s.parse()?),
                    _ => unsupported_sql_err!("Expected mode to be a string"),
                };
                Ok(decode(input.clone(), raise_on_error, mode))
            }
            _ => unsupported_sql_err!("Expected 2 arguments"),
        }
    }
}
