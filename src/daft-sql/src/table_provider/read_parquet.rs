use daft_core::prelude::TimeUnit;
use daft_logical_plan::LogicalPlanBuilder;
use daft_scan::builder::ParquetScanBuilder;
use sqlparser::ast::TableFunctionArgs;

use super::SQLTableFunction;
use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::SQLFunctionArguments,
    modules::config::expr_to_iocfg,
    planner::SQLPlanner,
};

pub(super) struct ReadParquetFunction;

impl TryFrom<SQLFunctionArguments> for ParquetScanBuilder {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let glob_paths: String = args.try_get_positional(0)?.ok_or_else(|| {
            PlannerError::invalid_operation("path is required for `read_parquet`")
        })?;
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
        let schema = None; // TODO
        let io_config = args.get_named("io_config").map(expr_to_iocfg).transpose()?;

        Ok(Self {
            glob_paths: vec![glob_paths],
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
                // "schema",
                // "field_id_mapping",
                // "row_groups",
                "io_config",
            ],
            1, // 1 positional argument (path)
        )?;

        builder.finish().map_err(From::from)
    }
}
