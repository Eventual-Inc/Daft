use daft_logical_plan::LogicalPlanBuilder;
use daft_scan::builder::CsvScanBuilder;
use sqlparser::ast::TableFunctionArgs;

use super::SQLTableFunction;
use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::SQLFunctionArguments,
    modules::config::expr_to_iocfg,
    planner::SQLPlanner,
};

pub(super) struct ReadCsvFunction;

impl TryFrom<SQLFunctionArguments> for CsvScanBuilder {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let delimiter = args.try_get_named("delimiter")?;
        let has_headers: bool = args.try_get_named("has_headers")?.unwrap_or(true);
        let double_quote: bool = args.try_get_named("double_quote")?.unwrap_or(true);
        let quote = args.try_get_named("quote")?;
        let escape_char = args.try_get_named("escape_char")?;
        let comment = args.try_get_named("comment")?;
        let allow_variable_columns = args
            .try_get_named("allow_variable_columns")?
            .unwrap_or(false);
        let glob_paths: String = match args.try_get_positional(0) {
            Ok(Some(path)) => path,
            Ok(None) => {
                // If the positional argument is not found, try to get the path from the named argument
                match args.try_get_named("path") {
                    Ok(Some(path)) => path,
                    Ok(None) => {
                        return Err(PlannerError::invalid_operation(
                            "path is required for `read_csv`",
                        ));
                    }
                    Err(err) => return Err(err),
                }
            }
            Err(err) => return Err(err),
        };
        let infer_schema = args.try_get_named("infer_schema")?.unwrap_or(true);
        let chunk_size = args.try_get_named("chunk_size")?;
        let buffer_size = args.try_get_named("buffer_size")?;
        let file_path_column = args.try_get_named("file_path_column")?;
        let hive_partitioning = args.try_get_named("hive_partitioning")?.unwrap_or(false);
        let use_native_downloader = args.try_get_named("use_native_downloader")?.unwrap_or(true);
        let schema = None; // TODO
        let schema_hints = None; // TODO
        let io_config = args.get_named("io_config").map(expr_to_iocfg).transpose()?;

        Ok(Self {
            glob_paths: vec![glob_paths],
            infer_schema,
            schema,
            has_headers,
            delimiter,
            double_quote,
            quote,
            escape_char,
            comment,
            allow_variable_columns,
            io_config,
            file_path_column,
            hive_partitioning,
            use_native_downloader,
            schema_hints,
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
                // "schema",
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
                "use_native_downloader",
                // "schema_hints",
                "buffer_size",
                "chunk_size",
            ],
            1, // 1 positional argument (path)
        )?;

        builder.finish().map_err(From::from)
    }
}
