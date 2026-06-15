#[cfg(test)]
use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
#[cfg(test)]
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
    async fn send(
        &self,
        record: KafkaOutgoingRecord,
        queue_timeout: Duration,
    ) -> DaftResult<KafkaDelivery>;

    async fn flush(&self, timeout: Duration) -> DaftResult<()>;

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

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct FakeProducer {
    sent_records: Mutex<Vec<KafkaOutgoingRecord>>,
    delivery_results: Mutex<VecDeque<DaftResult<KafkaDelivery>>>,
    flush_result: Mutex<Option<DaftResult<()>>>,
    next_offset: Mutex<i64>,
}

#[cfg(test)]
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

#[cfg(test)]
pub(crate) fn delivery_error(message: impl Into<String>) -> DaftError {
    DaftError::External(format!("[write_kafka] {}", message.into()).into())
}

#[cfg(feature = "kafka")]
pub(crate) struct KafkaExternalError<E> {
    context: String,
    source: E,
}

#[cfg(feature = "kafka")]
impl<E> KafkaExternalError<E> {
    pub(crate) fn new(context: impl Into<String>, source: E) -> Self {
        Self {
            context: context.into(),
            source,
        }
    }
}

#[cfg(feature = "kafka")]
impl<E: std::fmt::Display> std::fmt::Display for KafkaExternalError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[write_kafka] {}: {}", self.context, self.source)
    }
}

#[cfg(feature = "kafka")]
impl<E: std::fmt::Debug> std::fmt::Debug for KafkaExternalError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaExternalError")
            .field("context", &self.context)
            .field("source", &self.source)
            .finish()
    }
}

#[cfg(feature = "kafka")]
impl<E> std::error::Error for KafkaExternalError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(feature = "kafka")]
fn external_error<E>(context: impl Into<String>, source: E) -> DaftError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DaftError::External(Box::new(KafkaExternalError::new(context, source)))
}

#[cfg(feature = "kafka")]
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
                external_error(
                    format!(
                        "delivery failed for topic {:?}, partition {}, offset {}",
                        message.topic(),
                        message.partition(),
                        message.offset()
                    ),
                    err,
                )
            })
    }

    async fn flush(&self, timeout: Duration) -> DaftResult<()> {
        self.producer
            .flush(timeout)
            .map_err(|err| external_error("flush failed", err))
    }
}

#[cfg(all(test, feature = "kafka"))]
mod kafka_error_tests {
    use std::{error::Error, io};

    use common_error::DaftError;

    use super::KafkaExternalError;

    #[test]
    fn kafka_external_error_preserves_source_and_context() {
        let err = KafkaExternalError::new(
            "delivery failed for topic \"topic\", partition 1, offset 2",
            io::Error::other("broker unavailable"),
        );

        assert_eq!(
            err.to_string(),
            "[write_kafka] delivery failed for topic \"topic\", partition 1, offset 2: broker unavailable"
        );
        assert_eq!(err.source().unwrap().to_string(), "broker unavailable");
    }

    #[test]
    fn daft_external_error_exposes_kafka_wrapper_and_source() {
        let err = DaftError::External(Box::new(KafkaExternalError::new(
            "flush failed",
            io::Error::other("broker unavailable"),
        )));

        assert_eq!(
            err.to_string(),
            "DaftError::External [write_kafka] flush failed: broker unavailable"
        );
        let kafka_source = err.source().unwrap();
        assert_eq!(
            kafka_source.to_string(),
            "[write_kafka] flush failed: broker unavailable"
        );
        assert_eq!(
            kafka_source.source().unwrap().to_string(),
            "broker unavailable"
        );
    }
}

#[cfg(test)]
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
