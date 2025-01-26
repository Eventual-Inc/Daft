use daft_dsl::{Expr, ExprRef, LiteralValue};
use daft_functions::uri::{self, upload::UrlUploadArgs};
use sqlparser::ast::FunctionArg;

use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{self, SQLFunction, SQLFunctionArguments},
    unsupported_sql_err, SQLPlanner,
};

/// The Daft-SQL `url_upload` definition.
pub struct SqlUrlUpload;

impl TryFrom<SQLFunctionArguments> for UrlUploadArgs {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let max_connections: usize = args.try_get_named("max_connections")?.unwrap_or(32);
        let raise_error_on_failure = functions::args::parse_on_error(&args)?;
        let io_config = functions::args::parse_io_config(&args)?;
        Ok(Self {
            max_connections,
            raise_error_on_failure,
            multi_thread: true, // TODO always true
            is_single_folder: true,
            io_config: io_config.into(),
        })
    }
}

impl SQLFunction for SqlUrlUpload {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, location] => {
                let input = planner.plan_function_arg(input)?;
                let location = planner.plan_function_arg(location)?;
                Ok(uri::upload(input, location, None))
            }
            [input, location, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let location = planner.plan_function_arg(location)?;
                let mut args: UrlUploadArgs = planner.plan_function_args(
                    args,
                    &["max_connections", "on_error", "io_config"],
                    0,
                )?;
                // TODO consider moving the calculation of "is_single_folder" deeper so that both the SQL and Python sides don't compute this independently.
                // is_single_folder = true iff the location is a string.
                // in python, this is isinstance(location, str)
                // We perform the check here (with mut args) because TryFrom<SQLFunctionArguments> does not have access to `location`.
                args.is_single_folder =
                    matches!(location.as_ref(), Expr::Literal(LiteralValue::Utf8(_)));
                Ok(uri::upload(input, location, Some(args)))
            }
            _ => unsupported_sql_err!("Invalid arguments for url_upload: '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Uploads a column of binary data to the provided location(s) (also supports S3, local etc)."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "input",
            "location",
            "max_connections",
            "on_error",
            "io_config",
        ]
    }
}
