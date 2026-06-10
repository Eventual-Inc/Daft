use std::collections::BTreeMap;

use common_error::{DaftError, DaftResult};
use daft_logical_plan::sink_info::KafkaConfigValue;

const DEFAULT_CLIENT_ID: &str = "daft-write-kafka";
const REDACTED: &str = "<redacted>";

pub fn config_value_to_string(value: &KafkaConfigValue) -> String {
    match value {
        KafkaConfigValue::String(value) => value.clone(),
        KafkaConfigValue::Int(value) => value.to_string(),
        KafkaConfigValue::Float(value) => value.0.to_string(),
        KafkaConfigValue::Bool(value) => value.to_string(),
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

pub fn redact_config_value(key: &str, value: &str) -> String {
    let key = key.to_ascii_lowercase();
    if key.contains("password")
        || key == "sasl.oauthbearer.config"
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
) -> DaftResult<rdkafka::ClientConfig> {
    let mut client_config = rdkafka::ClientConfig::new();
    client_config
        .set("bootstrap.servers", bootstrap_servers)
        .set("client.id", DEFAULT_CLIENT_ID);

    for (key, value) in kafka_client_config {
        validate_config_key(key)?;
        let value = config_value_to_string(value);
        client_config.set(key, value);
    }

    Ok(client_config)
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

        let unsupported = validate_config_key("transactional.id").unwrap_err();
        assert!(unsupported.to_string().contains("transactional.id"));
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
        assert_eq!(redact_config_value("api.token", "abc"), "<redacted>");
        assert_eq!(redact_config_value("client.secret", "abc"), "<redacted>");
        assert_eq!(redact_config_value("acks", "all"), "all");
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn builds_client_config_with_managed_bootstrap_and_default_client_id() {
        let client_config = build_client_config("localhost:9092", &BTreeMap::new()).unwrap();

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

        let client_config = build_client_config("localhost:9092", &user_config).unwrap();

        assert_eq!(client_config.get("client.id"), Some("custom-client"));
        assert_eq!(client_config.get("acks"), Some("all"));
    }
}
