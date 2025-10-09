use daft_logical_plan::LogicalPlanBuilder;
use sqlparser::ast::TableFunctionArgs;

use super::{SQLTableFunction, expr_to_iocfg};
use crate::{SQLPlanner, error::SQLPlannerResult, unsupported_sql_err};

pub(super) struct ReadDeltalakeFunction;

#[cfg(feature = "python")]
impl SQLTableFunction for ReadDeltalakeFunction {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        let (uri, io_config) = match args.args.as_slice() {
            [uri] => (uri, None),
            [uri, io_config] => {
                let args = planner.parse_function_args(
                    std::slice::from_ref(io_config),
                    &["io_config"],
                    0,
                )?;
                let io_config = args.get_named("io_config").map(expr_to_iocfg).transpose()?;
                (uri, io_config)
            }
            _ => unsupported_sql_err!("Expected one or two arguments"),
        };
        let uri = planner.plan_function_arg(uri)?.into_inner();

        let Some(uri) = uri.as_literal().and_then(|lit| lit.as_str()) else {
            unsupported_sql_err!("Expected a string literal for the first argument");
        };

        daft_scan::builder::delta_scan(uri, io_config, true).map_err(From::from)
    }
}

#[cfg(not(feature = "python"))]
impl SQLTableFunction for ReadDeltalakeFunction {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        unsupported_sql_err!(
            "`read_deltalake` function is not supported. Enable the `python` feature to use this function."
        )
    }
}
