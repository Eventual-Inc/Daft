use std::sync::Arc;

use daft_dsl::{Expr, ExprRef, LiteralValue};
use daft_functions::uri::{download, download::DownloadFunction, upload, upload::UploadFunction};
use sqlparser::ast::FunctionArg;

use super::SQLModule;
use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctionArguments, SQLFunctions},
    modules::config::expr_to_iocfg,
    planner::SQLPlanner,
    unsupported_sql_err,
};

pub struct SQLModuleURL;

const DEFAULT_MAX_CONNECTIONS: usize = 32;

impl SQLModule for SQLModuleURL {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("url_download", UrlDownload);
        parent.add_fn("url_upload", UrlUpload);
    }
}

impl TryFrom<SQLFunctionArguments> for DownloadFunction {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let max_connections = args
            .try_get_named("max_connections")?
            .unwrap_or(DEFAULT_MAX_CONNECTIONS);
        let raise_error_on_failure = args
            .get_named("on_error")
            .map(|arg| match arg.as_ref() {
                Expr::Literal(LiteralValue::Utf8(s)) => match s.as_ref() {
                    "raise" => Ok(true),
                    "null" => Ok(false),
                    other => unsupported_sql_err!(
                        "Expected on_error to be 'raise' or 'null'; instead got '{other:?}'"
                    ),
                },
                other => unsupported_sql_err!(
                    "Expected on_error to be 'raise' or 'null'; instead got '{other:?}'"
                ),
            })
            .transpose()?
            .unwrap_or(true);

        // TODO: choice multi_thread based on the current engine (such as ray)
        let multi_thread = args.try_get_named("multi_thread")?.unwrap_or(false);

        let config = Arc::new(
            args.get_named("io_config")
                .map(expr_to_iocfg)
                .transpose()?
                .unwrap_or_default(),
        );

        Ok(Self {
            max_connections,
            raise_error_on_failure,
            multi_thread,
            config,
        })
    }
}

struct UrlDownload;

impl SQLFunction for UrlDownload {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let args: DownloadFunction = planner.plan_function_args(
                    args,
                    &["max_connections", "on_error", "multi_thread", "io_config"],
                    0,
                )?;

                Ok(download(
                    input,
                    args.max_connections,
                    args.raise_error_on_failure,
                    args.multi_thread,
                    Some(match Arc::try_unwrap(args.config) {
                        Ok(elem) => elem,
                        Err(elem) => (*elem).clone(),
                    }), // download requires Option<IOConfig>
                ))
            }
            _ => unsupported_sql_err!("Invalid arguments for url_download: '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _: &str) -> String {
        "download data from the given url".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "input",
            "max_connections",
            "on_error",
            "multi_thread",
            "io_config",
        ]
    }
}

impl TryFrom<SQLFunctionArguments> for UploadFunction {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let max_connections = args
            .try_get_named("max_connections")?
            .unwrap_or(DEFAULT_MAX_CONNECTIONS);

        let raise_error_on_failure = args
            .get_named("on_error")
            .map(|arg| match arg.as_ref() {
                Expr::Literal(LiteralValue::Utf8(s)) => match s.as_ref() {
                    "raise" => Ok(true),
                    "null" => Ok(false),
                    other => unsupported_sql_err!(
                        "Expected on_error to be 'raise' or 'null'; instead got '{other:?}'"
                    ),
                },
                other => unsupported_sql_err!(
                    "Expected on_error to be 'raise' or 'null'; instead got '{other:?}'"
                ),
            })
            .transpose()?
            .unwrap_or(true);

        // TODO: choice multi_thread based on the current engine (such as ray)
        let multi_thread = args.try_get_named("multi_thread")?.unwrap_or(false);

        // by default use row_specifc_urls
        let is_single_folder = false;

        let config = Arc::new(
            args.get_named("io_config")
                .map(expr_to_iocfg)
                .transpose()?
                .unwrap_or_default(),
        );

        Ok(Self {
            max_connections,
            raise_error_on_failure,
            multi_thread,
            is_single_folder,
            config,
        })
    }
}

struct UrlUpload;

impl SQLFunction for UrlUpload {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, location, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let location = planner.plan_function_arg(location)?;
                let mut args: UploadFunction = planner.plan_function_args(
                    args,
                    &["max_connections", "on_error", "multi_thread", "io_config"],
                    0,
                )?;
                if location.as_literal().is_some() {
                    args.is_single_folder = true;
                }
                Ok(upload(
                    input,
                    location,
                    args.max_connections,
                    args.raise_error_on_failure,
                    args.multi_thread,
                    args.is_single_folder,
                    Some(match Arc::try_unwrap(args.config) {
                        Ok(elem) => elem,
                        Err(elem) => (*elem).clone(),
                    }), // upload requires Option<IOConfig>
                ))
            }
            _ => unsupported_sql_err!("Invalid arguments for url_upload: '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _: &str) -> String {
        "upload data to the given path".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "input",
            "location",
            "max_connections",
            "on_error",
            "multi_thread",
            "io_config",
        ]
    }
}
