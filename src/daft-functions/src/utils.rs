use common_error::{DaftError, DaftResult};
use daft_core::prelude::{DataType, Field};
use daft_dsl::{Expr, ExprRef, LiteralValue};
use daft_io::{AzureConfig, GCSConfig, HTTPConfig, IOConfig, S3Config};

pub(crate) fn expr_to_iocfg(expr: &ExprRef) -> DaftResult<IOConfig> {
    // TODO(CORY): use serde to deserialize this
    let Expr::Literal(LiteralValue::Struct(entries)) = expr.as_ref() else {
        return Err(DaftError::ValueError("Invalid IOConfig".to_string()));
    };
    let f: DataType = todo!();
    macro_rules! get_value {
        ($field:literal, $type:ident) => {
            entries
                .get(&Field::new($field, DataType::$type))
                .and_then(|s| match s {
                    LiteralValue::$type(s) => Some(Ok(s.clone())),
                    LiteralValue::Null => None,
                    _ => Some(Err(DaftError::ValueError(format!(
                        "invalid field {} for IOConfig",
                        $field
                    )))),
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
        _ => {
            unreachable!("variant is required for IOConfig, this indicates a programming error")
        }
    }
}
