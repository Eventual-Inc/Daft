use std::sync::Arc;

use daft_core::prelude::TimeUnit;
use daft_logical_plan::LogicalPlanBuilder;
use daft_scan::builder::ParquetScanBuilder;
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

pub(super) struct ReadParquetFunction;

impl TryFrom<SQLFunctionArguments> for ParquetScanBuilder {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let glob_paths: Vec<String> = if let Some(arg) = args.get_positional(0) {
            try_coerce_list(arg.clone())?
        } else if let Some(arg) = args.get_named("path") {
            try_coerce_list(arg.clone())?
        } else {
            invalid_operation_err!("path is required for `read_json`")
        };

        let infer_schema = args.try_get_named("infer_schema")?.unwrap_or(true);
        let coerce_int96_timestamp_unit =
            args.try_get_named::<String>("coerce_int96_timestamp_unit")?;
        let coerce_int96_timestamp_unit: TimeUnit = coerce_int96_timestamp_unit
            .as_deref()
            .unwrap_or("nanoseconds")
            .parse::<TimeUnit>()
            .map_err(|_| {
                PlannerError::invalid_argument("coerce_int96_timestamp_unit", "read_parquet")
            })?;
        let chunk_size = args.try_get_named("chunk_size")?;
        let file_path_column = args.try_get_named("file_path_column")?;
        let multithreaded = args.try_get_named("multithreaded")?.unwrap_or(true);
        let hive_partitioning = args.try_get_named("hive_partitioning")?.unwrap_or(false);

        let field_id_mapping = None; // TODO
        let row_groups = None; // TODO
        let schema = args
            .try_get_named("schema")?
            .map(try_parse_schema)
            .transpose()?
            .map(Arc::new);
        let io_config = args.get_named("io_config").map(expr_to_iocfg).transpose()?;

        Ok(Self {
            glob_paths,
            infer_schema,
            coerce_int96_timestamp_unit,
            field_id_mapping,
            row_groups,
            chunk_size,
            io_config,
            multithreaded,
            schema,
            file_path_column,
            hive_partitioning,
        })
    }
}

impl SQLTableFunction for ReadParquetFunction {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        let builder: ParquetScanBuilder = planner.plan_function_args(
            args.args.as_slice(),
            &[
                "infer_schema",
                "coerce_int96_timestamp_unit",
                "chunk_size",
                "multithreaded",
                "schema",
                // "field_id_mapping",
                // "row_groups",
                "io_config",
            ],
            1, // 1 positional argument (path)
        )?;

        let runtime = common_runtime::get_io_runtime(true);

        let result = runtime.block_within_async_context(builder.finish())??;
        Ok(result)
    }
}
