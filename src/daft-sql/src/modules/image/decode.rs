use daft_dsl::{Expr, ExprRef, LiteralValue};
use daft_functions::image::decode::{decode, ImageDecode};
use sqlparser::ast::FunctionArg;

use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{self, SQLFunction, SQLFunctionArguments},
    unsupported_sql_err,
};

pub struct SQLImageDecode;

impl TryFrom<SQLFunctionArguments> for ImageDecode {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let mode = args
            .get_named("mode")
            .map(|arg| match arg.as_ref() {
                Expr::Literal(LiteralValue::Utf8(s)) => s.parse().map_err(PlannerError::from),
                _ => unsupported_sql_err!("Expected mode to be a string"),
            })
            .transpose()?;
        let raise_on_error = functions::args::parse_on_error(&args)?;
        Ok(Self {
            mode,
            raise_on_error,
        })
    }
}

impl SQLFunction for SQLImageDecode {
    fn to_expr(
        &self,
        inputs: &[FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?.into_inner();
                Ok(decode(input, None))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?.into_inner();
                let args = planner.plan_function_args(args, &["mode", "on_error"], 0)?;
                Ok(decode(input, Some(args)))
            }
            _ => unsupported_sql_err!("Invalid arguments for image_decode: '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Decodes an image from binary data. Optionally, you can specify the image mode and error handling behavior.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "mode", "on_error"]
    }
}
