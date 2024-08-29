use daft_core::datatypes::ImageFormat;
use daft_dsl::{Expr, ExprRef, LiteralValue};
use sqlparser::ast::{FunctionArg, FunctionArgOperator};

use crate::{
    error::SQLPlannerResult,
    functions::{FromSQLArgs, SQLFunction},
    unsupported_sql_err,
};
use daft_functions::image::encode::{encode, ImageEncode};

pub struct SQLImageEncode;

impl FromSQLArgs for ImageEncode {
    fn from_sql(
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<Self> {
        let mut image_format = None;

        for arg in inputs {
            match arg {
                FunctionArg::Named {
                    name,
                    arg,
                    operator: FunctionArgOperator::Assignment,
                } => match name.value.as_ref() {
                    "image_format" => {
                        let arg = planner.try_unwrap_function_arg_expr(arg)?;
                        image_format = Some(match arg.as_ref() {
                            Expr::Literal(LiteralValue::Utf8(s)) => s.parse()?,
                            _ => unsupported_sql_err!("Expected mode to be a string"),
                        });
                    }
                    name => unsupported_sql_err!("Unexpected argument: '{name}'"),
                },

                other => {
                    unsupported_sql_err!("Invalid arguments for image_decode: '{}'", other)
                }
            }
        }
        if image_format.is_none() {
            unsupported_sql_err!("Expected image_format to be a string");
        }

        Ok(Self {
            image_format: image_format.unwrap(),
        })
    }
}

impl SQLFunction for SQLImageEncode {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(encode(input, None))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let args = ImageEncode::from_sql(args, planner)?;
                Ok(encode(input, Some(args)))
            }
            _ => unsupported_sql_err!("Invalid arguments for image_decode: '{inputs:?}'"),
        }
    }
}
