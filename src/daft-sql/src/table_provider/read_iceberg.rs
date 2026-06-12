use common_io_config::IOConfig;
use daft_logical_plan::LogicalPlanBuilder;
use sqlparser::ast::TableFunctionArgs;

use super::SQLTableFunction;
use crate::{
    SQLPlanner,
    error::{PlannerError, SQLPlannerResult},
    functions::{self, SQLFunctionArguments},
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
}

impl SqlReadIcebergArgs {
    /// Like a TryFrom<SQLFunctionArguments> but from TalbeFunctionArgs directly and passing the planner.
    fn try_from(planner: &SQLPlanner, args: &TableFunctionArgs) -> SQLPlannerResult<Self> {
        planner.plan_function_args(
            &args.args,
            &["snapshot_id", "branch", "tag", "io_config"],
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
        let io_config: Option<IOConfig> = functions::args::parse_io_config(&args)?.into();
        Ok(Self {
            metadata_location,
            snapshot_id,
            branch,
            tag,
            io_config,
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
