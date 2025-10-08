use std::sync::Arc;

use daft_logical_plan::LogicalPlanBuilder;
use daft_scan::builder::CsvScanBuilder;
use sqlparser::ast::TableFunctionArgs;

use super::{SQLTableFunction, try_coerce_list};
use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::SQLFunctionArguments,
    invalid_operation_err,
    modules::config::expr_to_iocfg,
    planner::SQLPlanner,
    schema::try_parse_schema,
};

pub(super) struct ReadCsvFunction;

impl TryFrom<SQLFunctionArguments> for CsvScanBuilder {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        // TODO validations (unsure if should carry over from python API)
        // - ensure infer_schema is true if schema is None.

        let glob_paths: Vec<String> = if let Some(arg) = args.get_positional(0) {
            try_coerce_list(arg.clone())?
        } else if let Some(arg) = args.get_named("path") {
            try_coerce_list(arg.clone())?
        } else {
            invalid_operation_err!("path is required for `read_csv`")
        };

        let delimiter = args.try_get_named("delimiter")?;
        let has_headers: bool = args.try_get_named("has_headers")?.unwrap_or(true);
        let double_quote: bool = args.try_get_named("double_quote")?.unwrap_or(true);
        let quote = args.try_get_named("quote")?;
        let escape_char = args.try_get_named("escape_char")?;
        let comment = args.try_get_named("comment")?;
        let allow_variable_columns = args
            .try_get_named("allow_variable_columns")?
            .unwrap_or(false);
        let infer_schema = args.try_get_named("infer_schema")?.unwrap_or(true);
        let chunk_size = args.try_get_named("chunk_size")?;
        let buffer_size = args.try_get_named("buffer_size")?;
        let file_path_column = args.try_get_named("file_path_column")?;
        let hive_partitioning = args.try_get_named("hive_partitioning")?.unwrap_or(false);
        let schema = args
            .try_get_named("schema")?
            .map(try_parse_schema)
            .transpose()?
            .map(Arc::new);
        let io_config = args.get_named("io_config").map(expr_to_iocfg).transpose()?;

        Ok(Self {
            glob_paths,
            infer_schema,
            io_config,
            schema,
            file_path_column,
            hive_partitioning,
            delimiter,
            has_headers,
            double_quote,
            quote,
            escape_char,
            comment,
            allow_variable_columns,
            buffer_size,
            chunk_size,
        })
    }
}

impl SQLTableFunction for ReadCsvFunction {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        let builder: CsvScanBuilder = planner.plan_function_args(
            args.args.as_slice(),
            &[
                "path",
                "infer_schema",
                "schema",
                "has_headers",
                "delimiter",
                "double_quote",
                "quote",
                "escape_char",
                "comment",
                "allow_variable_columns",
                "io_config",
                "file_path_column",
                "hive_partitioning",
                "buffer_size",
                "chunk_size",
            ],
            1, // 1 positional argument (path)
        )?;

        let runtime = common_runtime::get_io_runtime(true);
        let result = runtime.block_within_async_context(builder.finish())??;
        Ok(result)
    }
}
