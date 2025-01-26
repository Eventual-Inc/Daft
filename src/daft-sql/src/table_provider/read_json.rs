use daft_scan::builder::JsonScanBuilder;

use super::{expr_to_iocfg, SQLTableFunction};
use crate::{error::PlannerError, functions::SQLFunctionArguments};

pub(super) struct ReadJsonFunction;

impl SQLTableFunction for ReadJsonFunction {
    fn plan(
        &self,
        planner: &crate::SQLPlanner,
        args: &sqlparser::ast::TableFunctionArgs,
    ) -> crate::error::SQLPlannerResult<daft_logical_plan::LogicalPlanBuilder> {
        let builder: JsonScanBuilder = planner.plan_function_args(
            args.args.as_slice(),
            &[
                "path",
                "infer_schema",
                // "schema"
                "io_config",
                "file_path_column",
                "hive_partitioning",
                // "schema_hints",
                "buffer_size",
                "chunk_size",
            ],
            1, // (path)
        )?;
        let runtime = common_runtime::get_io_runtime(true);
        let result = runtime.block_on(builder.finish())??;
        Ok(result)
    }
}

impl TryFrom<SQLFunctionArguments> for JsonScanBuilder {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        // TODO validations (unsure if should carry over from python API)
        // - schema_hints is deprecated
        // - ensure infer_schema is true if schema is None.

        let glob_paths: String = args
            .try_get_positional(0)?
            .ok_or_else(|| PlannerError::invalid_operation("path is required for `read_json`"))?;

        let infer_schema = args.try_get_named("infer_schema")?.unwrap_or(true);
        let chunk_size = args.try_get_named("chunk_size")?;
        let buffer_size = args.try_get_named("buffer_size")?;
        let file_path_column = args.try_get_named("file_path_column")?;
        let hive_partitioning = args.try_get_named("hive_partitioning")?.unwrap_or(false);
        let schema = None; // TODO
        let schema_hints = None; // TODO
        let io_config = args.get_named("io_config").map(expr_to_iocfg).transpose()?;

        Ok(Self {
            glob_paths: vec![glob_paths],
            infer_schema,
            schema,
            io_config,
            file_path_column,
            hive_partitioning,
            schema_hints,
            buffer_size,
            chunk_size,
        })
    }
}
