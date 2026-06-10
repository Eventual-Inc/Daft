use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use parking_lot::Mutex;

use super::metrics::KafkaProducerMetrics;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KafkaHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KafkaOutgoingRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<KafkaHeader>,
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
}

impl KafkaOutgoingRecord {
    pub(crate) fn delivered_bytes(&self) -> usize {
        let record_bytes =
            self.key.as_ref().map_or(0, Vec::len) + self.value.as_ref().map_or(0, Vec::len);

        self.headers.iter().fold(record_bytes, |bytes, header| {
            bytes + header.key.len() + header.value.as_ref().map_or(0, Vec::len)
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KafkaDelivery {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
}

#[async_trait]
pub(crate) trait KafkaProducer: Send + Sync {
    async fn send(&self, record: KafkaOutgoingRecord) -> DaftResult<KafkaDelivery>;

    async fn flush(&self, timeout: Duration) -> DaftResult<()>;

    fn metrics_snapshot(&self) -> Option<KafkaProducerMetrics> {
        None
    }
}

#[derive(Debug, Default)]
pub(crate) struct FakeProducer {
    sent_records: Mutex<Vec<KafkaOutgoingRecord>>,
    delivery_results: Mutex<VecDeque<DaftResult<KafkaDelivery>>>,
    flush_result: Mutex<Option<DaftResult<()>>>,
    next_offset: Mutex<i64>,
}

impl FakeProducer {
    pub(crate) fn succeeding() -> Self {
        Self::default()
    }

    pub(crate) fn with_delivery_results(results: Vec<DaftResult<KafkaDelivery>>) -> Self {
        Self {
            delivery_results: Mutex::new(results.into()),
            ..Self::default()
        }
    }

    pub(crate) fn with_flush_result(result: DaftResult<()>) -> Self {
        Self {
            flush_result: Mutex::new(Some(result)),
            ..Self::default()
        }
    }

    pub(crate) fn sent_records(&self) -> Vec<KafkaOutgoingRecord> {
        self.sent_records.lock().clone()
    }
}

pub(crate) fn delivery_error(message: impl Into<String>) -> DaftError {
    DaftError::ValueError(format!("[write_kafka] {}", message.into()))
}

#[async_trait]
impl KafkaProducer for FakeProducer {
    async fn send(&self, record: KafkaOutgoingRecord) -> DaftResult<KafkaDelivery> {
        self.sent_records.lock().push(record.clone());

        if let Some(result) = self.delivery_results.lock().pop_front() {
            return result;
        }

        let mut next_offset = self.next_offset.lock();
        let delivery = KafkaDelivery {
            topic: record.topic,
            partition: record.partition.unwrap_or(0),
            offset: *next_offset,
            timestamp_ms: record.timestamp_ms,
        };
        *next_offset = next_offset.saturating_add(1);

        Ok(delivery)
    }

    async fn flush(&self, _timeout: Duration) -> DaftResult<()> {
        self.flush_result.lock().take().unwrap_or(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn record(
        topic: &str,
        partition: Option<i32>,
        timestamp_ms: Option<i64>,
    ) -> KafkaOutgoingRecord {
        KafkaOutgoingRecord {
            topic: topic.to_string(),
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            headers: vec![KafkaHeader {
                key: "header".to_string(),
                value: Some(b"bytes".to_vec()),
            }],
            partition,
            timestamp_ms,
        }
    }

    #[test]
    fn delivered_bytes_counts_key_value_and_headers() {
        let record = KafkaOutgoingRecord {
            topic: "topic".to_string(),
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            headers: vec![
                KafkaHeader {
                    key: "a".to_string(),
                    value: Some(b"bc".to_vec()),
                },
                KafkaHeader {
                    key: "null".to_string(),
                    value: None,
                },
            ],
            partition: None,
            timestamp_ms: None,
        };

        assert_eq!(record.delivered_bytes(), 3 + 5 + 1 + 2 + 4);
    }

    #[tokio::test]
    async fn succeeding_fake_records_sent_records_and_deliveries() {
        let producer = FakeProducer::succeeding();
        let first = record("topic", Some(2), Some(123));
        let second = record("topic", None, None);

        let first_delivery = producer.send(first.clone()).await.unwrap();
        let second_delivery = producer.send(second.clone()).await.unwrap();

        assert_eq!(producer.sent_records(), vec![first, second]);
        assert_eq!(
            first_delivery,
            KafkaDelivery {
                topic: "topic".to_string(),
                partition: 2,
                offset: 0,
                timestamp_ms: Some(123),
            }
        );
        assert_eq!(second_delivery.offset, 1);
        assert_eq!(second_delivery.partition, 0);
        assert!(producer.metrics_snapshot().is_none());
    }

    #[tokio::test]
    async fn fake_uses_queued_delivery_results_fifo() {
        let queued = KafkaDelivery {
            topic: "queued".to_string(),
            partition: 4,
            offset: 99,
            timestamp_ms: Some(7),
        };
        let producer = FakeProducer::with_delivery_results(vec![
            Ok(queued.clone()),
            Err(delivery_error("delivery failed")),
        ]);

        assert_eq!(
            producer.send(record("ignored", None, None)).await.unwrap(),
            queued
        );

        let err = producer
            .send(record("still-recorded", None, None))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("[write_kafka] delivery failed"));
        assert_eq!(producer.sent_records().len(), 2);
    }

    #[tokio::test]
    async fn fake_flush_can_return_configured_result_once() {
        let producer = FakeProducer::with_flush_result(Err(delivery_error("flush failed")));

        let err = producer.flush(Duration::from_millis(1)).await.unwrap_err();
        assert!(err.to_string().contains("[write_kafka] flush failed"));
        producer.flush(Duration::from_millis(1)).await.unwrap();
    }
}
