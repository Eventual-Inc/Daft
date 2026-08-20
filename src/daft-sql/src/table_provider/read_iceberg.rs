use common_io_config::IOConfig;
use daft_logical_plan::LogicalPlanBuilder;
use sqlparser::ast::TableFunctionArgs;

use super::SQLTableFunction;
use crate::{
    SQLPlanner,
    error::{PlannerError, SQLPlannerResult},
    functions::SQLFunctionArguments,
    modules::config::expr_to_iocfg,
};

/// The Daft-SQL `read_iceberg` table-value function.
pub(super) struct SqlReadIceberg;

/// The Daft-SQL `read_iceberg` table-value function arguments.
struct SqlReadIcebergArgs {
    metadata_location: String,
    snapshot_id: Option<usize>,
    branch: Option<String>,
    tag: Option<String>,
    io_config: Option<IOConfig>,
    ignore_corrupt_files: bool,
}

impl SqlReadIcebergArgs {
    /// Like a TryFrom<SQLFunctionArguments> but from TalbeFunctionArgs directly and passing the planner.
    fn try_from(planner: &SQLPlanner, args: &TableFunctionArgs) -> SQLPlannerResult<Self> {
        planner.plan_function_args(
            &args.args,
            &[
                "snapshot_id",
                "branch",
                "tag",
                "io_config",
                "ignore_corrupt_files",
            ],
            1,
        )
    }
}

impl TryFrom<SQLFunctionArguments> for SqlReadIcebergArgs {
    type Error = PlannerError;

    /// This is required to use `planner.plan_function_args`
    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let metadata_location: String = args
            .try_get_positional(0)?
            .expect("read_iceberg requires a path");
        let snapshot_id: Option<usize> = args.try_get_named("snapshot_id")?;
        let branch: Option<String> = args.try_get_named("branch")?;
        let tag: Option<String> = args.try_get_named("tag")?;
        // Keep `None` when unset so the scan can fall back to the table's FileIO
        // properties and the context `default_io_config`, matching the Python API.
        let io_config: Option<IOConfig> =
            args.get_named("io_config").map(expr_to_iocfg).transpose()?;
        let ignore_corrupt_files = args.try_get_named("ignore_corrupt_files")?.unwrap_or(false);
        Ok(Self {
            metadata_location,
            snapshot_id,
            branch,
            tag,
            io_config,
            ignore_corrupt_files,
        })
    }
}

/// Translates the `read_iceberg` table-value function to a logical scan operator.
#[cfg(feature = "python")]
impl SQLTableFunction for SqlReadIceberg {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        let args = SqlReadIcebergArgs::try_from(planner, args)?;
        Ok(daft_logical_plan::scan_builder::iceberg_scan(
            args.metadata_location,
            args.snapshot_id,
            args.branch,
            args.tag,
            args.io_config,
            args.ignore_corrupt_files,
        )?)
    }
}

/// Translates the `read_iceberg` table-value function to a logical scan operator (errors without python feature).
#[cfg(not(feature = "python"))]
impl SQLTableFunction for SqlReadIceberg {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        crate::unsupported_sql_err!(
            "`read_iceberg` function is not supported. Enable the `python` feature to use this function."
        )
    }
}
