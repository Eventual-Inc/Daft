use std::{collections::HashMap, sync::Arc};

use common_io_config::{HTTPConfig, IOConfig, S3Config};
use daft_core::prelude::{DataType, Field, TimeUnit};
use daft_dsl::{Expr, ExprRef, LiteralValue};
use daft_plan::{LogicalPlanBuilder, ParquetScanBuilder};
use once_cell::sync::Lazy;
use sqlparser::ast::{TableAlias, TableFunctionArgs};

use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::SQLFunctionArguments,
    planner::{Relation, SQLPlanner},
    unsupported_sql_err,
};

pub(crate) static SQL_TABLE_FUNCTIONS: Lazy<SQLTableFunctions> = Lazy::new(|| {
    let mut functions = SQLTableFunctions::new();
    functions.add_fn("read_parquet", ReadParquetFunction);

    functions
});

/// TODOs
///   - Use multimap for function variants.
///   - Add more functions..
pub struct SQLTableFunctions {
    pub(crate) map: HashMap<String, Arc<dyn SQLTableFunction>>,
}

impl SQLTableFunctions {
    /// Create a new [SQLFunctions] instance.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    /// Add a [FunctionExpr] to the [SQLFunctions] instance.
    pub(crate) fn add_fn<F: SQLTableFunction + 'static>(&mut self, name: &str, func: F) {
        self.map.insert(name.to_string(), Arc::new(func));
    }

    /// Get a function by name from the [SQLFunctions] instance.
    pub(crate) fn get(&self, name: &str) -> Option<&Arc<dyn SQLTableFunction>> {
        self.map.get(name)
    }
}

impl SQLPlanner {
    pub(crate) fn plan_table_function(
        &self,
        fn_name: &str,
        args: &TableFunctionArgs,
        alias: &Option<TableAlias>,
    ) -> SQLPlannerResult<Relation> {
        let fns = &SQL_TABLE_FUNCTIONS;

        let Some(func) = fns.get(fn_name) else {
            unsupported_sql_err!("Function `{}` not found", fn_name);
        };

        let builder = func.plan(self, args)?;
        let name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| fn_name.to_string());

        Ok(Relation::new(builder, name))
    }
}

pub(crate) trait SQLTableFunction: Send + Sync {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder>;
}

struct ReadParquetFunction;

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
            .parse()
            .map_err(|_| {
                PlannerError::invalid_argument("coerce_int96_timestamp_unit", "read_parquet")
            })?;
        let chunk_size = args.try_get_named("chunk_size")?;
        let multithreaded = args.try_get_named("multithreaded")?.unwrap_or(true);

        let field_id_mapping = None; // TODO
        let row_groups = None; // TODO
        let io_config = args.get_named("io_config").map(expr_to_iocfg).transpose()?;

        let schema = None; // TODO

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

fn expr_to_iocfg(expr: &ExprRef) -> SQLPlannerResult<IOConfig> {
    let Expr::Literal(LiteralValue::Struct(entries)) = expr.as_ref() else {
        unsupported_sql_err!("Invalid IOConfig");
    };

    macro_rules! get_value {
        ($field:literal, $type:ident) => {
            entries
                .get(&Field::new($field, DataType::$type))
                .and_then(|s| match s {
                    LiteralValue::$type(s) => Some(Ok(s.clone())),
                    LiteralValue::Null => None,
                    _ => Some(Err(PlannerError::invalid_argument($field, "IOConfig"))),
                })
                .transpose()
        };
    }

    let variant = get_value!("variant", Utf8)?
        .expect("variant is required for IOConfig, this indicates a programming error");

    match variant.as_ref() {
        "s3" => {
            let region_name = get_value!("region_name", Utf8)?;
            let endpoint_url = get_value!("endpoint_url", Utf8)?;
            let key_id = get_value!("key_id", Utf8)?;
            let session_token = get_value!("session_token", Utf8)?.map(|s| s.into());
            let access_key = get_value!("access_key", Utf8)?.map(|s| s.into());
            let buffer_time = get_value!("buffer_time", UInt64)?;
            let max_connections_per_io_thread =
                get_value!("max_connections_per_io_thread", UInt32)?;
            let retry_initial_backoff_ms = get_value!("retry_initial_backoff_ms", UInt64)?;
            let connect_timeout_ms = get_value!("connect_timeout_ms", UInt64)?;
            let read_timeout_ms = get_value!("read_timeout_ms", UInt64)?;
            let num_tries = get_value!("num_tries", UInt32)?;
            let retry_mode = get_value!("retry_mode", Utf8)?;
            let anonymous = get_value!("anonymous", Boolean)?;
            let use_ssl = get_value!("use_ssl", Boolean)?;
            let verify_ssl = get_value!("verify_ssl", Boolean)?;
            let check_hostname_ssl = get_value!("check_hostname_ssl", Boolean)?;
            let requester_pays = get_value!("requester_pays", Boolean)?;
            let force_virtual_addressing = get_value!("force_virtual_addressing", Boolean)?;
            let profile_name = get_value!("profile_name", Utf8)?;
            let default = S3Config::default();
            let s3_config = S3Config {
                region_name,
                endpoint_url,
                key_id,
                session_token,
                access_key,
                credentials_provider: None,
                buffer_time,
                max_connections_per_io_thread: max_connections_per_io_thread
                    .unwrap_or(default.max_connections_per_io_thread),
                retry_initial_backoff_ms: retry_initial_backoff_ms
                    .unwrap_or(default.retry_initial_backoff_ms),
                connect_timeout_ms: connect_timeout_ms.unwrap_or(default.connect_timeout_ms),
                read_timeout_ms: read_timeout_ms.unwrap_or(default.read_timeout_ms),
                num_tries: num_tries.unwrap_or(default.num_tries),
                retry_mode,
                anonymous: anonymous.unwrap_or(default.anonymous),
                use_ssl: use_ssl.unwrap_or(default.use_ssl),
                verify_ssl: verify_ssl.unwrap_or(default.verify_ssl),
                check_hostname_ssl: check_hostname_ssl.unwrap_or(default.check_hostname_ssl),
                requester_pays: requester_pays.unwrap_or(default.requester_pays),
                force_virtual_addressing: force_virtual_addressing
                    .unwrap_or(default.force_virtual_addressing),
                profile_name,
            };

            Ok(IOConfig {
                s3: s3_config,
                ..Default::default()
            })
        }
        "http" => {
            let default = HTTPConfig::default();
            let user_agent = get_value!("user_agent", Utf8)?.unwrap_or(default.user_agent);
            let bearer_token = get_value!("bearer_token", Utf8)?.map(|s| s.into());

            Ok(IOConfig {
                http: HTTPConfig {
                    user_agent,
                    bearer_token,
                },
                ..Default::default()
            })
        }
        _ => {
            unsupported_sql_err!("Unsupported IOConfig variant: {}", variant);
        }
    }
}
