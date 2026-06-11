use std::collections::BTreeMap;

use common_error::{DaftError, DaftResult};
use daft_logical_plan::sink_info::KafkaConfigValue;

const DEFAULT_CLIENT_ID: &str = "daft-write-kafka";
const DELIVERY_TIMEOUT_MS: &str = "delivery.timeout.ms";
const MESSAGE_TIMEOUT_MS: &str = "message.timeout.ms";
// TODO: Use this in user-facing Kafka diagnostics when config logging lands.
#[allow(dead_code)]
const REDACTED: &str = "<redacted>";

pub fn config_value_to_string(value: &KafkaConfigValue) -> String {
    match value {
        KafkaConfigValue::String(value) => value.clone(),
        KafkaConfigValue::Int(value) => value.to_string(),
        KafkaConfigValue::Float(value) => value.0.to_string(),
        KafkaConfigValue::Bool(value) => value.to_string(),
        // librdkafka represents configuration values as strings; Daft nulls intentionally
        // map to an empty value so users can pass through configs that use empty strings.
        KafkaConfigValue::Null => String::new(),
    }
}

pub fn validate_config_key(key: &str) -> DaftResult<()> {
    match key.to_ascii_lowercase().as_str() {
        "bootstrap.servers" => Err(DaftError::ValueError(
            "[write_kafka] kafka_client_config must not override managed key: \
             \"bootstrap.servers\""
                .to_string(),
        )),
        "transactional.id" => Err(DaftError::not_implemented(
            "[write_kafka] transactional.id is not supported yet",
        )),
        _ => Ok(()),
    }
}

// TODO: Use this in user-facing Kafka diagnostics when config logging lands.
#[allow(dead_code)]
pub fn redact_config_value(key: &str, value: &str) -> String {
    let key = key.to_ascii_lowercase();
    if key == "sasl.jaas.config"
        || key == "basic.auth.user.info"
        || key == "sasl.oauthbearer.config"
        || key.contains("password")
        || key.contains("credential")
        || key.contains("secret")
        || key.contains("token")
    {
        REDACTED.to_string()
    } else {
        value.to_string()
    }
}

#[cfg(feature = "kafka")]
pub fn build_client_config(
    bootstrap_servers: &str,
    kafka_client_config: &BTreeMap<String, KafkaConfigValue>,
    timeout_ms: u64,
) -> DaftResult<rdkafka::ClientConfig> {
    let mut client_config = rdkafka::ClientConfig::new();
    client_config
        .set("bootstrap.servers", bootstrap_servers)
        .set("client.id", DEFAULT_CLIENT_ID);
    if !has_user_delivery_timeout(kafka_client_config) {
        client_config.set(DELIVERY_TIMEOUT_MS, timeout_ms.to_string());
    }

    for (key, value) in kafka_client_config {
        validate_config_key(key)?;
        let value = config_value_to_string(value);
        client_config.set(key, value);
    }

    Ok(client_config)
}

#[cfg(feature = "kafka")]
fn has_user_delivery_timeout(kafka_client_config: &BTreeMap<String, KafkaConfigValue>) -> bool {
    kafka_client_config.keys().any(|key| {
        let key = key.to_ascii_lowercase();
        key == DELIVERY_TIMEOUT_MS || key == MESSAGE_TIMEOUT_MS
    })
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "kafka")]
    use std::collections::BTreeMap;

    use common_hashable_float_wrapper::FloatWrapper;

    use super::*;

    #[test]
    fn converts_scalar_config_values_to_strings() {
        assert_eq!(
            config_value_to_string(&KafkaConfigValue::String("all".to_string())),
            "all"
        );
        assert_eq!(config_value_to_string(&KafkaConfigValue::Int(42)), "42");
        assert_eq!(
            config_value_to_string(&KafkaConfigValue::Float(FloatWrapper(1.25))),
            "1.25"
        );
        assert_eq!(
            config_value_to_string(&KafkaConfigValue::Bool(true)),
            "true"
        );
        assert_eq!(
            config_value_to_string(&KafkaConfigValue::Bool(false)),
            "false"
        );
        assert_eq!(config_value_to_string(&KafkaConfigValue::Null), "");
    }

    #[test]
    fn validates_managed_and_unsupported_config_keys() {
        assert!(validate_config_key("acks").is_ok());
        assert!(validate_config_key("client.id").is_ok());

        let managed = validate_config_key("bootstrap.servers").unwrap_err();
        assert!(managed.to_string().contains("bootstrap.servers"));
        let managed_mixed_case = validate_config_key("BOOTSTRAP.SERVERS").unwrap_err();
        assert!(managed_mixed_case.to_string().contains("bootstrap.servers"));

        let unsupported = validate_config_key("transactional.id").unwrap_err();
        assert!(unsupported.to_string().contains("transactional.id"));
        let unsupported_mixed_case = validate_config_key("Transactional.ID").unwrap_err();
        assert!(
            unsupported_mixed_case
                .to_string()
                .contains("transactional.id")
        );
    }

    #[test]
    fn redacts_sensitive_config_values() {
        assert_eq!(
            redact_config_value("sasl.password", "super-secret"),
            "<redacted>"
        );
        assert_eq!(
            redact_config_value("sasl.oauthbearer.config", "token=abc"),
            "<redacted>"
        );
        assert_eq!(
            redact_config_value("SASL.JAAS.CONFIG", "username=alice password=abc"),
            "<redacted>"
        );
        assert_eq!(
            redact_config_value("Basic.Auth.User.Info", "alice:abc"),
            "<redacted>"
        );
        assert_eq!(
            redact_config_value("Sasl.OAuthBearer.Config", "token=abc"),
            "<redacted>"
        );
        assert_eq!(redact_config_value("api.token", "abc"), "<redacted>");
        assert_eq!(redact_config_value("client.secret", "abc"), "<redacted>");
        assert_eq!(
            redact_config_value("schema.registry.credential.source", "user-info"),
            "<redacted>"
        );
        assert_eq!(redact_config_value("acks", "all"), "all");
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn builds_client_config_with_managed_bootstrap_and_default_client_id() {
        let client_config =
            build_client_config("localhost:9092", &BTreeMap::new(), 10_000).unwrap();

        assert_eq!(
            client_config.get("bootstrap.servers"),
            Some("localhost:9092")
        );
        assert_eq!(client_config.get("client.id"), Some(DEFAULT_CLIENT_ID));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn build_client_config_allows_client_id_override() {
        let mut user_config = BTreeMap::new();
        user_config.insert(
            "client.id".to_string(),
            KafkaConfigValue::String("custom-client".to_string()),
        );
        user_config.insert(
            "acks".to_string(),
            KafkaConfigValue::String("all".to_string()),
        );

        let client_config = build_client_config("localhost:9092", &user_config, 10_000).unwrap();

        assert_eq!(client_config.get("client.id"), Some("custom-client"));
        assert_eq!(client_config.get("acks"), Some("all"));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn build_client_config_sets_default_delivery_timeout_from_write_timeout() {
        let client_config =
            build_client_config("localhost:9092", &BTreeMap::new(), 12_345).unwrap();

        assert_eq!(client_config.get("delivery.timeout.ms"), Some("12345"));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn build_client_config_preserves_delivery_timeout_override() {
        let mut user_config = BTreeMap::new();
        user_config.insert(
            "delivery.timeout.ms".to_string(),
            KafkaConfigValue::Int(60_000),
        );

        let client_config = build_client_config("localhost:9092", &user_config, 12_345).unwrap();

        assert_eq!(client_config.get("delivery.timeout.ms"), Some("60000"));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn build_client_config_preserves_legacy_message_timeout_override() {
        let mut user_config = BTreeMap::new();
        user_config.insert(
            "Message.Timeout.Ms".to_string(),
            KafkaConfigValue::Int(60_000),
        );

        let client_config = build_client_config("localhost:9092", &user_config, 12_345).unwrap();

        assert_eq!(client_config.get("Message.Timeout.Ms"), Some("60000"));
        assert_eq!(client_config.get("delivery.timeout.ms"), None);
    }
}
