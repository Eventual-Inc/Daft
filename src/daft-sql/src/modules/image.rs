use daft_dsl::{Expr, ExprRef, LiteralValue};
use sqlparser::ast::{FunctionArg, FunctionArgOperator};

use super::SQLModule;
use crate::{
    error::SQLPlannerResult,
    functions::{FromSQLArgs, SQLFunction, SQLFunctions},
    unsupported_sql_err,
};
use daft_functions::image::decode::{decode, ImageDecode};

pub struct SQLModuleImage;

impl SQLModule for SQLModuleImage {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("image_decode", SQLImageDecode {});
    }
}

pub struct SQLImageDecode;

impl FromSQLArgs for ImageDecode {
    fn from_sql(
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<Self> {
        let mut mode = None;
        let mut raise_on_error = true;
        for arg in inputs {
            match arg {
                FunctionArg::Named {
                    name,
                    arg,
                    operator: FunctionArgOperator::Assignment,
                } => match name.value.as_ref() {
                    "mode" => {
                        let arg = planner.try_unwrap_function_arg_expr(arg)?;
                        mode = Some(match arg.as_ref() {
                            Expr::Literal(LiteralValue::Utf8(s)) => s.parse()?,
                            _ => unsupported_sql_err!("Expected mode to be a string"),
                        });
                    }
                    "on_error" => {
                        let arg = planner.try_unwrap_function_arg_expr(arg)?;

                        raise_on_error = match arg.as_ref() {
                            Expr::Literal(LiteralValue::Utf8(s)) => match s.as_ref() {
                                "raise" => true,
                                "null" => false,
                                _ => unsupported_sql_err!(
                                    "Expected on_error to be 'raise' or 'null'"
                                ),
                            },
                            _ => unsupported_sql_err!("Expected raise_on_error to be a boolean"),
                        };
                    }
                    name => unsupported_sql_err!("Unexpected argument: '{name}'"),
                },

                other => {
                    unsupported_sql_err!("Invalid arguments for image_decode: '{}'", other)
                }
            }
        }

        Ok(Self {
            mode,
            raise_on_error,
        })
    }
}

impl SQLFunction for SQLImageDecode {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(decode(input, None))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let args = ImageDecode::from_sql(args, planner)?;
                Ok(decode(input, Some(args)))
            }
            _ => unsupported_sql_err!("Invalid arguments for image_decode: '{inputs:?}'"),
        }
    }
}
