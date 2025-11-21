use common_io_config::{AzureConfig, GCSConfig, HTTPConfig, IOConfig, S3Config, TosConfig};
use daft_core::prelude::*;
use daft_dsl::{Expr, ExprRef};

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
        parent.add_fn("AzureConfig", AzureConfigFunction);
        parent.add_fn("GCSConfig", GCSConfigFunction);
        parent.add_fn("TosConfig", TosConfigFunction);
    }
}

pub struct S3ConfigFunction;
macro_rules! item {
    ($name:expr) => {
        (stringify!($name).to_string(), $name.into())
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
                "multipart_size",
                "multipart_max_concurrency",
                "custom_retry_msgs",
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

        let multipart_size = args.try_get_named("multipart_size")?.map(|t: i64| t as u64);
        let multipart_max_concurrency = args
            .try_get_named("multipart_max_concurrency")?
            .map(|t: i64| t as u32);
        let custom_retry_msgs = args.try_get_named::<String>("custom_retry_msgs")?;

        let entries = vec![
            ("variant".to_string(), "s3".into()),
            item!(region_name),
            item!(endpoint_url),
            item!(key_id),
            item!(session_token),
            item!(access_key),
            item!(buffer_time),
            item!(max_connections_per_io_thread),
            item!(retry_initial_backoff_ms),
            item!(connect_timeout_ms),
            item!(read_timeout_ms),
            item!(num_tries),
            item!(retry_mode),
            item!(anonymous),
            item!(use_ssl),
            item!(verify_ssl),
            item!(check_hostname_ssl),
            item!(requester_pays),
            item!(force_virtual_addressing),
            item!(profile_name),
            item!(multipart_size),
            item!(multipart_max_concurrency),
            item!(custom_retry_msgs),
        ]
        .into_iter()
        .collect::<_>();

        Ok(Expr::Literal(Literal::Struct(entries)).arced())
    }
    fn docstrings(&self, _: &str) -> String {
        "Create configurations to be used when accessing an S3-compatible system.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
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
            "multipart_size",
            "multipart_max_concurrency",
            "custom_retry_msgs",
        ]
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

        let retry_initial_backoff_ms = args
            .try_get_named::<usize>("retry_initial_backoff_ms")?
            .map(|t: usize| t as u64);
        let connect_timeout_ms = args
            .try_get_named::<usize>("connect_timeout_ms")?
            .map(|t: usize| t as u64);
        let read_timeout_ms = args
            .try_get_named::<usize>("read_timeout_ms")?
            .map(|t: usize| t as u64);
        let num_tries = args
            .try_get_named::<usize>("num_tries")?
            .map(|t: usize| t as u32);

        let entries = vec![
            ("variant".to_string(), "http".into()),
            item!(user_agent),
            item!(bearer_token),
            item!(retry_initial_backoff_ms),
            item!(connect_timeout_ms),
            item!(read_timeout_ms),
            item!(num_tries),
        ]
        .into_iter()
        .collect::<_>();

        Ok(Expr::Literal(Literal::Struct(entries)).arced())
    }

    fn docstrings(&self, _: &str) -> String {
        "Create configurations for sending web requests.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "user_agent",
            "bearer_token",
            "retry_initial_backoff_ms",
            "connect_timeout_ms",
            "read_timeout_ms",
            "num_tries",
        ]
    }
}
pub struct AzureConfigFunction;
impl SQLFunction for AzureConfigFunction {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        let args: SQLFunctionArguments = planner.parse_function_args(
            inputs,
            &[
                "storage_account",
                "access_key",
                "sas_token",
                "bearer_token",
                "tenant_id",
                "client_id",
                "client_secret",
                "use_fabric_endpoint",
                "anonymous",
                "endpoint_url",
                "use_ssl",
            ],
            0,
        )?;

        let storage_account = args.try_get_named::<String>("storage_account")?;
        let access_key = args.try_get_named::<String>("access_key")?;
        let sas_token = args.try_get_named::<String>("sas_token")?;
        let bearer_token = args.try_get_named::<String>("bearer_token")?;
        let tenant_id = args.try_get_named::<String>("tenant_id")?;
        let client_id = args.try_get_named::<String>("client_id")?;
        let client_secret = args.try_get_named::<String>("client_secret")?;
        let use_fabric_endpoint = args.try_get_named::<bool>("use_fabric_endpoint")?;
        let anonymous = args.try_get_named::<bool>("anonymous")?;
        let endpoint_url = args.try_get_named::<String>("endpoint_url")?;
        let use_ssl = args.try_get_named::<bool>("use_ssl")?;

        let entries = vec![
            ("variant".to_string(), "azure".into()),
            item!(storage_account),
            item!(access_key),
            item!(sas_token),
            item!(bearer_token),
            item!(tenant_id),
            item!(client_id),
            item!(client_secret),
            item!(use_fabric_endpoint),
            item!(anonymous),
            item!(endpoint_url),
            item!(use_ssl),
        ]
        .into_iter()
        .collect::<_>();

        Ok(Expr::Literal(Literal::Struct(entries)).arced())
    }

    fn docstrings(&self, _: &str) -> String {
        "Create configurations to be used when accessing Azure Blob Storage.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "storage_account",
            "access_key",
            "sas_token",
            "bearer_token",
            "tenant_id",
            "client_id",
            "client_secret",
            "use_fabric_endpoint",
            "anonymous",
            "endpoint_url",
            "use_ssl",
        ]
    }
}

pub struct GCSConfigFunction;

impl SQLFunction for GCSConfigFunction {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        let args: SQLFunctionArguments = planner.parse_function_args(
            inputs,
            &["project_id", "credentials", "token", "anonymous"],
            0,
        )?;

        let project_id = args.try_get_named::<String>("project_id")?;
        let credentials = args.try_get_named::<String>("credentials")?;
        let token = args.try_get_named::<String>("token")?;
        let anonymous = args.try_get_named::<bool>("anonymous")?;

        let entries = vec![
            ("variant".to_string(), "gcs".into()),
            item!(project_id),
            item!(credentials),
            item!(token),
            item!(anonymous),
        ]
        .into_iter()
        .collect::<_>();

        Ok(Expr::Literal(Literal::Struct(entries)).arced())
    }
    fn docstrings(&self, _: &str) -> String {
        "Create configurations to be used when accessing Google Cloud Storage.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["project_id", "credentials", "token", "anonymous"]
    }
}

pub struct TosConfigFunction;

impl SQLFunction for TosConfigFunction {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        let args: SQLFunctionArguments = planner.parse_function_args(
            inputs,
            &[
                "region",
                "endpoint",
                "access_key",
                "secret_key",
                "security_token",
                "anonymous",
                "max_retries",
                "retry_timeout_ms",
                "connect_timeout_ms",
                "read_timeout_ms",
                "max_concurrent_requests",
                "max_connections_per_io_thread",
            ],
            0,
        )?;

        let region = args.try_get_named::<String>("region")?;
        let endpoint = args.try_get_named::<String>("endpoint")?;
        let access_key = args.try_get_named::<String>("access_key")?;
        let secret_key = args.try_get_named::<String>("secret_key")?;
        let security_token = args.try_get_named::<String>("security_token")?;
        let anonymous = args.try_get_named::<bool>("anonymous")?;
        let max_retries = args
            .try_get_named::<usize>("max_retries")?
            .map(|t| t as u32);
        let retry_timeout_ms = args
            .try_get_named::<usize>("retry_timeout_ms")?
            .map(|t| t as u64);
        let connect_timeout_ms = args
            .try_get_named::<usize>("connect_timeout_ms")?
            .map(|t| t as u64);
        let read_timeout_ms = args
            .try_get_named::<usize>("read_timeout_ms")?
            .map(|t| t as u64);
        let max_concurrent_requests = args
            .try_get_named::<usize>("max_concurrent_requests")?
            .map(|t| t as u32);
        let max_connections_per_io_thread = args
            .try_get_named::<usize>("max_connections_per_io_thread")?
            .map(|t| t as u32);

        let entries = vec![
            ("variant".to_string(), "tos".into()),
            item!(region),
            item!(endpoint),
            item!(access_key),
            item!(secret_key),
            item!(security_token),
            item!(anonymous),
            item!(max_retries),
            item!(retry_timeout_ms),
            item!(connect_timeout_ms),
            item!(read_timeout_ms),
            item!(max_concurrent_requests),
            item!(max_connections_per_io_thread),
        ]
        .into_iter()
        .collect::<_>();

        Ok(Expr::Literal(Literal::Struct(entries)).arced())
    }

    fn docstrings(&self, _: &str) -> String {
        "Create configurations to be used when accessing TOS (Torch Object Storage).".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "region",
            "endpoint",
            "access_key",
            "secret_key",
            "security_token",
            "anonymous",
            "max_retries",
            "retry_timeout_ms",
            "connect_timeout_ms",
            "read_timeout_ms",
            "max_concurrent_requests",
            "max_connections_per_io_thread",
        ]
    }
}

pub(crate) fn expr_to_iocfg(expr: &ExprRef) -> SQLPlannerResult<IOConfig> {
    // TODO(CORY): use serde to deserialize this
    let Expr::Literal(Literal::Struct(entries)) = expr.as_ref() else {
        unsupported_sql_err!("Invalid IOConfig");
    };

    macro_rules! get_value {
        ($field:literal, $type:ident) => {
            entries
                .get($field)
                .and_then(|s| match s {
                    Literal::$type(s) => Some(Ok(s.clone())),
                    Literal::Null => None,
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
            let multipart_size = get_value!("multipart_size", UInt64)?;
            let multipart_max_concurrency = get_value!("multipart_max_concurrency", UInt32)?;
            let custom_retry_msgs_str: Option<String> = get_value!("custom_retry_msgs", Utf8)?;
            let custom_retry_msgs =
                custom_retry_msgs_str.map(|s| s.split(',').map(|s| s.into()).collect());

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
                multipart_size: multipart_size.unwrap_or(default.multipart_size),
                multipart_max_concurrency: multipart_max_concurrency
                    .unwrap_or(default.multipart_max_concurrency),
                custom_retry_msgs: custom_retry_msgs.unwrap_or(default.custom_retry_msgs),
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
            let retry_initial_backoff_ms = get_value!("retry_initial_backoff_ms", UInt64)?
                .unwrap_or(default.retry_initial_backoff_ms);
            let connect_timeout_ms =
                get_value!("connect_timeout_ms", UInt64)?.unwrap_or(default.connect_timeout_ms);
            let read_timeout_ms =
                get_value!("read_timeout_ms", UInt64)?.unwrap_or(default.read_timeout_ms);
            let num_tries = get_value!("num_tries", UInt32)?.unwrap_or(default.num_tries);

            Ok(IOConfig {
                http: HTTPConfig {
                    user_agent,
                    bearer_token,
                    retry_initial_backoff_ms,
                    connect_timeout_ms,
                    read_timeout_ms,
                    num_tries,
                },
                ..Default::default()
            })
        }
        "azure" => {
            let storage_account = get_value!("storage_account", Utf8)?;
            let access_key = get_value!("access_key", Utf8)?;
            let sas_token = get_value!("sas_token", Utf8)?;
            let bearer_token = get_value!("bearer_token", Utf8)?;
            let tenant_id = get_value!("tenant_id", Utf8)?;
            let client_id = get_value!("client_id", Utf8)?;
            let client_secret = get_value!("client_secret", Utf8)?;
            let use_fabric_endpoint = get_value!("use_fabric_endpoint", Boolean)?;
            let anonymous = get_value!("anonymous", Boolean)?;
            let endpoint_url = get_value!("endpoint_url", Utf8)?;
            let use_ssl = get_value!("use_ssl", Boolean)?;

            let default = AzureConfig::default();

            Ok(IOConfig {
                azure: AzureConfig {
                    storage_account,
                    access_key: access_key.map(|s| s.into()),
                    sas_token,
                    bearer_token,
                    tenant_id,
                    client_id,
                    client_secret: client_secret.map(|s| s.into()),
                    use_fabric_endpoint: use_fabric_endpoint.unwrap_or(default.use_fabric_endpoint),
                    anonymous: anonymous.unwrap_or(default.anonymous),
                    endpoint_url,
                    use_ssl: use_ssl.unwrap_or(default.use_ssl),
                },
                ..Default::default()
            })
        }
        "gcs" => {
            let project_id = get_value!("project_id", Utf8)?;
            let credentials = get_value!("credentials", Utf8)?;
            let token = get_value!("token", Utf8)?;
            let anonymous = get_value!("anonymous", Boolean)?;
            let max_connections_per_io_thread =
                get_value!("max_connections_per_io_thread", UInt32)?;
            let retry_initial_backoff_ms = get_value!("retry_initial_backoff_ms", UInt64)?;
            let connect_timeout_ms = get_value!("connect_timeout_ms", UInt64)?;
            let read_timeout_ms = get_value!("read_timeout_ms", UInt64)?;
            let num_tries = get_value!("num_tries", UInt32)?;

            let default = GCSConfig::default();
            Ok(IOConfig {
                gcs: GCSConfig {
                    project_id,
                    credentials: credentials.map(|s| s.into()),
                    token,
                    anonymous: anonymous.unwrap_or(default.anonymous),
                    max_connections_per_io_thread: max_connections_per_io_thread
                        .unwrap_or(default.max_connections_per_io_thread),
                    retry_initial_backoff_ms: retry_initial_backoff_ms
                        .unwrap_or(default.retry_initial_backoff_ms),
                    connect_timeout_ms: connect_timeout_ms.unwrap_or(default.connect_timeout_ms),
                    read_timeout_ms: read_timeout_ms.unwrap_or(default.read_timeout_ms),
                    num_tries: num_tries.unwrap_or(default.num_tries),
                },
                ..Default::default()
            })
        }
        "tos" => {
            let region = get_value!("region", Utf8)?;
            let endpoint = get_value!("endpoint", Utf8)?;
            let access_key = get_value!("access_key", Utf8)?;
            let secret_key = get_value!("secret_key", Utf8)?.map(|s| s.into());
            let security_token = get_value!("security_token", Utf8)?.map(|s| s.into());
            let anonymous = get_value!("anonymous", Boolean)?;
            let max_retries = get_value!("max_retries", UInt32)?;
            let retry_timeout_ms = get_value!("retry_timeout_ms", UInt64)?;
            let connect_timeout_ms = get_value!("connect_timeout_ms", UInt64)?;
            let read_timeout_ms = get_value!("read_timeout_ms", UInt64)?;
            let max_concurrent_requests = get_value!("max_concurrent_requests", UInt32)?;
            let max_connections_per_io_thread =
                get_value!("max_connections_per_io_thread", UInt32)?;

            let default = TosConfig::default();
            Ok(IOConfig {
                tos: TosConfig {
                    region,
                    endpoint,
                    access_key,
                    secret_key,
                    security_token,
                    anonymous: anonymous.unwrap_or(default.anonymous),
                    max_retries: max_retries.unwrap_or(default.max_retries),
                    retry_timeout_ms: retry_timeout_ms.unwrap_or(default.retry_timeout_ms),
                    connect_timeout_ms: connect_timeout_ms.unwrap_or(default.connect_timeout_ms),
                    read_timeout_ms: read_timeout_ms.unwrap_or(default.read_timeout_ms),
                    max_concurrent_requests: max_concurrent_requests
                        .unwrap_or(default.max_concurrent_requests),
                    max_connections_per_io_thread: max_connections_per_io_thread
                        .unwrap_or(default.max_connections_per_io_thread),
                },
                ..Default::default()
            })
        }
        _ => {
            unreachable!("variant is required for IOConfig, this indicates a programming error")
        }
    }
}
