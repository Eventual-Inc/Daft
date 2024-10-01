use common_io_config::{HTTPConfig, IOConfig, S3Config};
use daft_core::prelude::{DataType, Field};
use daft_dsl::{literal_value, Expr, ExprRef, LiteralValue};

use super::SQLModule;
use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctionArguments, SQLFunctions},
    unsupported_sql_err,
};

pub struct SQLModuleConfig;

impl SQLModule for SQLModuleConfig {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("S3Config", S3ConfigFunction);
        parent.add_fn("HTTPConfig", HTTPConfigFunction);
        // parent.add_fn("AzureConfig", AzureConfigFunction);
        // parent.add_fn("GCSConfig", GCSConfigFunction);
    }
}

pub struct S3ConfigFunction;
macro_rules! item {
    ($name:expr, $ty:ident) => {
        (
            Field::new(stringify!($name), DataType::$ty),
            literal_value($name),
        )
    };
}

impl SQLFunction for S3ConfigFunction {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        // TODO(cory): Ideally we should use serde to deserialize the input arguments
        let args: SQLFunctionArguments = planner.parse_function_args(
            inputs,
            &[
                "region_name",
                "endpoint_url",
                "key_id",
                "session_token",
                "access_key",
                "credentials_provider",
                "buffer_time",
                "max_connections_per_io_thread",
                "retry_initial_backoff_ms",
                "connect_timeout_ms",
                "read_timeout_ms",
                "num_tries",
                "retry_mode",
                "anonymous",
                "use_ssl",
                "verify_ssl",
                "check_hostname_ssl",
                "requester_pays",
                "force_virtual_addressing",
                "profile_name",
            ],
            0,
        )?;

        let region_name = args.try_get_named::<String>("region_name")?;
        let endpoint_url = args.try_get_named::<String>("endpoint_url")?;
        let key_id = args.try_get_named::<String>("key_id")?;
        let session_token = args.try_get_named::<String>("session_token")?;

        let access_key = args.try_get_named::<String>("access_key")?;
        let buffer_time = args.try_get_named("buffer_time")?.map(|t: i64| t as u64);

        let max_connections_per_io_thread = args
            .try_get_named("max_connections_per_io_thread")?
            .map(|t: i64| t as u32);

        let retry_initial_backoff_ms = args
            .try_get_named("retry_initial_backoff_ms")?
            .map(|t: i64| t as u64);

        let connect_timeout_ms = args
            .try_get_named("connect_timeout_ms")?
            .map(|t: i64| t as u64);

        let read_timeout_ms = args
            .try_get_named("read_timeout_ms")?
            .map(|t: i64| t as u64);

        let num_tries = args.try_get_named("num_tries")?.map(|t: i64| t as u32);
        let retry_mode = args.try_get_named::<String>("retry_mode")?;
        let anonymous = args.try_get_named::<bool>("anonymous")?;
        let use_ssl = args.try_get_named::<bool>("use_ssl")?;
        let verify_ssl = args.try_get_named::<bool>("verify_ssl")?;
        let check_hostname_ssl = args.try_get_named::<bool>("check_hostname_ssl")?;
        let requester_pays = args.try_get_named::<bool>("requester_pays")?;
        let force_virtual_addressing = args.try_get_named::<bool>("force_virtual_addressing")?;
        let profile_name = args.try_get_named::<String>("profile_name")?;

        let entries = vec![
            (Field::new("variant", DataType::Utf8), literal_value("s3")),
            item!(region_name, Utf8),
            item!(endpoint_url, Utf8),
            item!(key_id, Utf8),
            item!(session_token, Utf8),
            item!(access_key, Utf8),
            item!(buffer_time, UInt64),
            item!(max_connections_per_io_thread, UInt32),
            item!(retry_initial_backoff_ms, UInt64),
            item!(connect_timeout_ms, UInt64),
            item!(read_timeout_ms, UInt64),
            item!(num_tries, UInt32),
            item!(retry_mode, Utf8),
            item!(anonymous, Boolean),
            item!(use_ssl, Boolean),
            item!(verify_ssl, Boolean),
            item!(check_hostname_ssl, Boolean),
            item!(requester_pays, Boolean),
            item!(force_virtual_addressing, Boolean),
            item!(profile_name, Utf8),
        ]
        .into_iter()
        .collect::<_>();

        Ok(Expr::Literal(LiteralValue::Struct(entries)).arced())
    }
}

pub struct HTTPConfigFunction;

impl SQLFunction for HTTPConfigFunction {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        let args: SQLFunctionArguments =
            planner.parse_function_args(inputs, &["user_agent", "bearer_token"], 0)?;

        let user_agent = args.try_get_named::<String>("user_agent")?;
        let bearer_token = args.try_get_named::<String>("bearer_token")?;

        let entries = vec![
            (Field::new("variant", DataType::Utf8), literal_value("http")),
            item!(user_agent, Utf8),
            item!(bearer_token, Utf8),
        ]
        .into_iter()
        .collect::<_>();

        Ok(Expr::Literal(LiteralValue::Struct(entries)).arced())
    }
}

pub(crate) fn expr_to_iocfg(expr: &ExprRef) -> SQLPlannerResult<IOConfig> {
    // TODO(CORY): use serde to deserialize this
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
