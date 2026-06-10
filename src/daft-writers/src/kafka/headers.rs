use common_error::{DaftError, DaftResult};

use super::producer::KafkaHeader;

// TODO(native-kafka-write): remove once Task 8/9 validates headers from writer integration.
#[allow(dead_code)]
pub(crate) fn validate_header_key(key: &str) -> DaftResult<()> {
    if key.is_empty() {
        return Err(DaftError::ValueError(
            "[write_kafka] kafka header key must not be empty".to_string(),
        ));
    }

    Ok(())
}

// TODO(native-kafka-write): remove once Task 8/9 builds headers from writer integration.
#[allow(dead_code)]
pub(crate) fn make_header(key: &str, value: Option<&[u8]>) -> DaftResult<KafkaHeader> {
    validate_header_key(key)?;

    Ok(KafkaHeader {
        key: key.to_string(),
        value: value.map(<[u8]>::to_vec),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_duplicate_keys_and_order() {
        let headers = vec![
            make_header("same", Some(b"first")).unwrap(),
            make_header("other", Some(b"middle")).unwrap(),
            make_header("same", Some(b"second")).unwrap(),
        ];

        assert_eq!(headers[0].key, "same");
        assert_eq!(headers[0].value.as_deref(), Some(&b"first"[..]));
        assert_eq!(headers[1].key, "other");
        assert_eq!(headers[1].value.as_deref(), Some(&b"middle"[..]));
        assert_eq!(headers[2].key, "same");
        assert_eq!(headers[2].value.as_deref(), Some(&b"second"[..]));
    }

    #[test]
    fn preserves_null_header_values() {
        let header = make_header("nullable", None).unwrap();

        assert_eq!(header.key, "nullable");
        assert_eq!(header.value, None);
    }

    #[test]
    fn rejects_empty_header_key() {
        let err = validate_header_key("").unwrap_err();

        assert!(err.to_string().contains("[write_kafka]"));
        assert!(err.to_string().contains("header key"));
    }
}
