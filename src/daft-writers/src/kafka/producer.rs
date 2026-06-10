use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use parking_lot::Mutex;
#[cfg(feature = "kafka")]
use rdkafka::{
    Message,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
};

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
    // TODO(native-kafka-write): remove once Task 9 records delivery accounting from writer projection.
    #[allow(dead_code)]
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
    // TODO(native-kafka-write): remove once Task 9 wires row projection to producer sends.
    #[allow(dead_code)]
    async fn send(
        &self,
        record: KafkaOutgoingRecord,
        queue_timeout: Duration,
    ) -> DaftResult<KafkaDelivery>;

    async fn flush(&self, timeout: Duration) -> DaftResult<()>;

    // TODO(native-kafka-write): remove once Task 9 decides producer metrics integration.
    #[allow(dead_code)]
    fn metrics_snapshot(&self) -> Option<KafkaProducerMetrics> {
        None
    }
}

#[cfg(feature = "kafka")]
#[derive(Clone)]
pub(crate) struct RdkafkaProducer {
    producer: FutureProducer,
}

#[cfg(feature = "kafka")]
impl RdkafkaProducer {
    pub(crate) fn new(producer: FutureProducer) -> Self {
        Self { producer }
    }
}

#[derive(Debug, Default)]
// TODO(native-kafka-write): remove once Task 9 consumes the fake in row-projection tests.
#[allow(dead_code)]
pub(crate) struct FakeProducer {
    sent_records: Mutex<Vec<KafkaOutgoingRecord>>,
    delivery_results: Mutex<VecDeque<DaftResult<KafkaDelivery>>>,
    flush_result: Mutex<Option<DaftResult<()>>>,
    next_offset: Mutex<i64>,
}

impl FakeProducer {
    // TODO(native-kafka-write): remove once Task 9 consumes the fake in row-projection tests.
    #[allow(dead_code)]
    pub(crate) fn succeeding() -> Self {
        Self::default()
    }

    // TODO(native-kafka-write): remove once Task 9 consumes the fake in row-projection tests.
    #[allow(dead_code)]
    pub(crate) fn with_delivery_results(results: Vec<DaftResult<KafkaDelivery>>) -> Self {
        Self {
            delivery_results: Mutex::new(results.into()),
            ..Self::default()
        }
    }

    // TODO(native-kafka-write): remove once Task 9 consumes the fake in row-projection tests.
    #[allow(dead_code)]
    pub(crate) fn with_flush_result(result: DaftResult<()>) -> Self {
        Self {
            flush_result: Mutex::new(Some(result)),
            ..Self::default()
        }
    }

    // TODO(native-kafka-write): remove once Task 9 consumes the fake in row-projection tests.
    #[allow(dead_code)]
    pub(crate) fn sent_records(&self) -> Vec<KafkaOutgoingRecord> {
        self.sent_records.lock().clone()
    }
}

// TODO(native-kafka-write): remove once Task 9 consumes fake producer failures from writer tests.
#[allow(dead_code)]
pub(crate) fn delivery_error(message: impl Into<String>) -> DaftError {
    DaftError::External(format!("[write_kafka] {}", message.into()).into())
}

#[cfg(feature = "kafka")]
fn external_error(message: impl Into<String>) -> DaftError {
    DaftError::External(format!("[write_kafka] {}", message.into()).into())
}

#[cfg(feature = "kafka")]
// TODO(native-kafka-write): remove once Task 9 exercises producer sends from writer projection.
#[allow(dead_code)]
fn to_owned_headers(headers: &[KafkaHeader]) -> Option<OwnedHeaders> {
    if headers.is_empty() {
        return None;
    }

    let mut owned_headers = OwnedHeaders::new_with_capacity(headers.len());
    for header in headers {
        owned_headers = owned_headers.insert(Header {
            key: &header.key,
            value: header.value.as_deref(),
        });
    }
    Some(owned_headers)
}

#[cfg(feature = "kafka")]
#[async_trait]
impl KafkaProducer for RdkafkaProducer {
    async fn send(
        &self,
        record: KafkaOutgoingRecord,
        queue_timeout: Duration,
    ) -> DaftResult<KafkaDelivery> {
        let mut future_record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(&record.topic);

        if let Some(key) = record.key.as_deref() {
            future_record = future_record.key(key);
        }
        if let Some(value) = record.value.as_deref() {
            future_record = future_record.payload(value);
        }
        if let Some(partition) = record.partition {
            future_record = future_record.partition(partition);
        }
        if let Some(timestamp_ms) = record.timestamp_ms {
            future_record = future_record.timestamp(timestamp_ms);
        }
        if let Some(headers) = to_owned_headers(&record.headers) {
            future_record = future_record.headers(headers);
        }

        self.producer
            .send(future_record, queue_timeout)
            .await
            .map(|delivery| KafkaDelivery {
                topic: record.topic,
                partition: delivery.partition,
                offset: delivery.offset,
                timestamp_ms: delivery.timestamp.to_millis(),
            })
            .map_err(|(err, message)| {
                external_error(format!(
                    "delivery failed for topic {:?}, partition {}, offset {}: {err}",
                    message.topic(),
                    message.partition(),
                    message.offset()
                ))
            })
    }

    async fn flush(&self, timeout: Duration) -> DaftResult<()> {
        self.producer
            .flush(timeout)
            .map_err(|err| external_error(format!("flush failed: {err}")))
    }
}

#[async_trait]
impl KafkaProducer for FakeProducer {
    async fn send(
        &self,
        record: KafkaOutgoingRecord,
        _queue_timeout: Duration,
    ) -> DaftResult<KafkaDelivery> {
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

    fn queue_timeout() -> Duration {
        Duration::from_millis(10)
    }

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

        let first_delivery = producer.send(first.clone(), queue_timeout()).await.unwrap();
        let second_delivery = producer
            .send(second.clone(), queue_timeout())
            .await
            .unwrap();

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
            producer
                .send(record("ignored", None, None), queue_timeout())
                .await
                .unwrap(),
            queued
        );

        let err = producer
            .send(record("still-recorded", None, None), queue_timeout())
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
