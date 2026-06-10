use common_error::{DaftError, DaftResult};

use super::producer::{KafkaHeader, KafkaOutgoingRecord};

pub(crate) fn validate_topic(topic: &str) -> DaftResult<()> {
    if topic.is_empty() {
        return Err(DaftError::ValueError(
            "[write_kafka] kafka topic must not be empty".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn validate_partition(partition: i32) -> DaftResult<()> {
    if partition < 0 {
        return Err(DaftError::ValueError(format!(
            "[write_kafka] kafka partition must be non-negative, got {partition}"
        )));
    }

    Ok(())
}

#[cfg(test)]
pub(crate) fn make_test_record(
    topic: &str,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: Vec<KafkaHeader>,
    partition: Option<i32>,
    timestamp_ms: Option<i64>,
) -> DaftResult<KafkaOutgoingRecord> {
    validate_topic(topic)?;
    if let Some(partition) = partition {
        validate_partition(partition)?;
    }

    Ok(KafkaOutgoingRecord {
        topic: topic.to_string(),
        key: key.map(<[u8]>::to_vec),
        value: value.map(<[u8]>::to_vec),
        headers,
        partition,
        timestamp_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::headers::make_header;

    #[test]
    fn makes_valid_record_and_counts_bytes() {
        let record = make_test_record(
            "topic",
            Some(b"key"),
            Some(b"value"),
            vec![make_header("h", Some(b"v")).unwrap()],
            Some(3),
            Some(456),
        )
        .unwrap();

        assert_eq!(record.topic, "topic");
        assert_eq!(record.key.as_deref(), Some(&b"key"[..]));
        assert_eq!(record.value.as_deref(), Some(&b"value"[..]));
        assert_eq!(record.partition, Some(3));
        assert_eq!(record.timestamp_ms, Some(456));
        assert_eq!(record.delivered_bytes(), 3 + 5 + 1 + 1);
    }

    #[test]
    fn supports_tombstone_value() {
        let record =
            make_test_record("topic", Some(b"key"), None, vec![], None, Some(789)).unwrap();

        assert_eq!(record.value, None);
        assert_eq!(record.timestamp_ms, Some(789));
        assert_eq!(record.delivered_bytes(), 3);
    }

    #[test]
    fn rejects_empty_topic() {
        let err = make_test_record("", None, Some(b"value"), vec![], None, None).unwrap_err();

        assert!(err.to_string().contains("[write_kafka]"));
        assert!(err.to_string().contains("topic"));
    }

    #[test]
    fn rejects_negative_partition() {
        let err =
            make_test_record("topic", None, Some(b"value"), vec![], Some(-1), None).unwrap_err();

        assert!(err.to_string().contains("[write_kafka]"));
        assert!(err.to_string().contains("partition"));
    }
}
