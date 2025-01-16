use super::SQLTableFunction;
use crate::unsupported_sql_err;

pub(super) struct ReadIcebergFunction;

#[cfg(feature = "python")]
impl SQLTableFunction for ReadIcebergFunction {
    fn plan(
        &self,
        _planner: &crate::SQLPlanner,
        _args: &sqlparser::ast::TableFunctionArgs,
    ) -> crate::error::SQLPlannerResult<daft_logical_plan::LogicalPlanBuilder> {
        unsupported_sql_err!("`read_iceberg` function is not implemented")
    }
}

#[cfg(not(feature = "python"))]
impl SQLTableFunction for ReadIcebergFunction {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        unsupported_sql_err!("`read_iceberg` function is not supported. Enable the `python` feature to use this function.")
    }
}
