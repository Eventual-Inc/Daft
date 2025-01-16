use daft_dsl::ExprRef;
use daft_functions::uri::{self, download::UrlDownloadArgs};
use sqlparser::ast::FunctionArg;

use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{self, SQLFunction, SQLFunctionArguments},
    unsupported_sql_err, SQLPlanner,
};

/// The Daft-SQL `url_download` definition.
pub struct SqlUrlDownload;

impl TryFrom<SQLFunctionArguments> for UrlDownloadArgs {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let max_connections: usize = args.try_get_named("max_connections")?.unwrap_or(32);
        let raise_error_on_failure = functions::args::parse_on_error(&args)?;
        let io_config = functions::args::parse_io_config(&args)?;
        Ok(Self {
            max_connections,
            raise_error_on_failure,
            multi_thread: true, // TODO always true
            io_config: io_config.into(),
        })
    }
}

impl SQLFunction for SqlUrlDownload {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(uri::download(input, None))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let args = planner.plan_function_args(
                    args,
                    &["max_connections", "on_error", "io_config"],
                    0,
                )?;
                Ok(uri::download(input, Some(args)))
            }
            _ => unsupported_sql_err!("Invalid arguments for url_download: '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Treats each string as a URL, and downloads the bytes contents as a bytes column."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "max_connections", "on_error", "io_config"]
    }
}
