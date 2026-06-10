use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::sink_info::KafkaWriteInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use rdkafka::producer::FutureProducer;

use super::{
    accounting::KafkaWriteAccounting,
    config::build_client_config,
    producer::{KafkaExternalError, KafkaProducer, RdkafkaProducer},
    record::records_from_micropartition,
};
use crate::{AsyncFileWriter, WriteResult, WriterFactory};

pub fn make_kafka_writer_factory(
    kafka_info: KafkaWriteInfo<BoundExpr>,
) -> Arc<dyn WriterFactory<Input = MicroPartition, Result = Vec<RecordBatch>>> {
    Arc::new(KafkaWriterFactory { kafka_info })
}

pub(crate) struct KafkaWriterFactory {
    kafka_info: KafkaWriteInfo<BoundExpr>,
}

impl WriterFactory for KafkaWriterFactory {
    type Input = MicroPartition;
    type Result = Vec<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        _partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        let producer: FutureProducer = build_client_config(
            &self.kafka_info.bootstrap_servers,
            &self.kafka_info.kafka_client_config,
        )?
        .create()
        .map_err(|err| {
            DaftError::External(Box::new(KafkaExternalError::new(
                "failed to create Kafka producer",
                err,
            )))
        })?;

        Ok(Box::new(KafkaWriter::new(
            self.kafka_info.clone(),
            RdkafkaProducer::new(producer),
            file_idx as i64,
        )))
    }
}

pub(crate) struct KafkaWriter<P: KafkaProducer> {
    kafka_info: KafkaWriteInfo<BoundExpr>,
    producer: P,
    accounting: KafkaWriteAccounting,
}

impl<P: KafkaProducer> KafkaWriter<P> {
    pub(crate) fn new(kafka_info: KafkaWriteInfo<BoundExpr>, producer: P, task_id: i64) -> Self {
        Self {
            kafka_info,
            producer,
            accounting: KafkaWriteAccounting::new(task_id),
        }
    }

    fn delivered_bytes(&self) -> usize {
        usize::try_from(self.accounting.bytes_delivered).unwrap_or(usize::MAX)
    }
}

#[async_trait]
impl<P: KafkaProducer> AsyncFileWriter for KafkaWriter<P> {
    type Input = MicroPartition;
    type Result = Vec<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<WriteResult> {
        let records = records_from_micropartition(&data, &self.kafka_info)?;
        let queue_timeout = Duration::from_millis(self.kafka_info.timeout_ms);
        let mut rows_written = 0usize;
        let mut bytes_written = 0usize;

        for record in records {
            self.accounting.record_attempt();
            let delivered_bytes = record.delivered_bytes();
            if let Err(err) = self.producer.send(record, queue_timeout).await {
                self.accounting.record_failure(err.to_string());
                return Err(err);
            }

            self.accounting.record_delivery(delivered_bytes);
            rows_written = rows_written.saturating_add(1);
            bytes_written = bytes_written.saturating_add(delivered_bytes);
        }

        Ok(WriteResult {
            rows_written,
            bytes_written,
        })
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        self.producer
            .flush(Duration::from_millis(self.kafka_info.timeout_ms))
            .await?;
        Ok(vec![self.accounting.to_record_batch()?])
    }

    fn bytes_written(&self) -> usize {
        self.delivered_bytes()
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.delivered_bytes()]
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use daft_core::prelude::{BinaryArray, DataType, Field, IntoSeries};
    use daft_dsl::bound_col;
    use daft_logical_plan::sink_info::{KafkaKeyFormat, KafkaTopic, KafkaValueFormat};

    use super::*;
    use crate::kafka::producer::{FakeProducer, delivery_error};

    fn kafka_info(timeout_ms: u64) -> KafkaWriteInfo<BoundExpr> {
        KafkaWriteInfo {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: KafkaTopic::Static("topic".to_string()),
            value_col: BoundExpr::new_unchecked(bound_col(
                1,
                Field::new("value", DataType::Binary),
            )),
            key_col: Some(BoundExpr::new_unchecked(bound_col(
                0,
                Field::new("key", DataType::Binary),
            ))),
            headers_col: None,
            partition: None,
            timestamp_ms_col: None,
            value_format: KafkaValueFormat::Raw,
            key_format: KafkaKeyFormat::Raw,
            kafka_client_config: BTreeMap::new(),
            timeout_ms,
        }
    }

    fn input_partition() -> MicroPartition {
        let batch = RecordBatch::from_nonempty_columns(vec![
            BinaryArray::from_iter("key", [Some(b"k1".as_slice()), Some(b"k2".as_slice())])
                .into_series(),
            BinaryArray::from_iter("value", [Some(b"v1".as_slice()), None]).into_series(),
        ])
        .unwrap();
        MicroPartition::new_loaded(batch.schema.clone(), Arc::new(vec![batch]), None)
    }

    #[tokio::test]
    async fn write_sends_projected_records_and_updates_bytes() {
        let mut writer = KafkaWriter::new(kafka_info(1), FakeProducer::succeeding(), 3);

        let result = writer.write(input_partition()).await.unwrap();

        assert_eq!(result.rows_written, 2);
        assert_eq!(result.bytes_written, 6);
        assert_eq!(writer.bytes_written(), 6);
        assert_eq!(writer.bytes_per_file(), vec![6]);
        let sent = writer.producer.sent_records();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].topic, "topic");
        assert_eq!(sent[0].key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(sent[0].value.as_deref(), Some(&b"v1"[..]));
        assert_eq!(sent[1].key.as_deref(), Some(&b"k2"[..]));
        assert_eq!(sent[1].value, None);

        let batches = writer.close().await.unwrap();
        assert_eq!(
            batches[0].columns()[1]
                .as_materialized_series()
                .i64()
                .unwrap()
                .get(0),
            Some(2)
        );
        assert_eq!(
            batches[0].columns()[2]
                .as_materialized_series()
                .i64()
                .unwrap()
                .get(0),
            Some(2)
        );
        assert_eq!(
            batches[0].columns()[4]
                .as_materialized_series()
                .i64()
                .unwrap()
                .get(0),
            Some(6)
        );
        assert!(!batches[0].columns()[5].as_materialized_series().is_valid(0));
    }

    #[tokio::test]
    async fn write_records_failure_accounting_and_returns_error() {
        let producer =
            FakeProducer::with_delivery_results(vec![Err(delivery_error("delivery failed"))]);
        let mut writer = KafkaWriter::new(kafka_info(1), producer, 3);

        let err = match writer.write(input_partition()).await {
            Ok(_) => panic!("KafkaWriter::write should fail when producer delivery fails"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("[write_kafka] delivery failed"));
        assert_eq!(writer.bytes_written(), 0);
        assert_eq!(writer.producer.sent_records().len(), 1);
        let batches = writer.close().await.unwrap();
        assert_eq!(
            batches[0].columns()[1]
                .as_materialized_series()
                .i64()
                .unwrap()
                .get(0),
            Some(1)
        );
        assert_eq!(
            batches[0].columns()[2]
                .as_materialized_series()
                .i64()
                .unwrap()
                .get(0),
            Some(0)
        );
        assert_eq!(
            batches[0].columns()[3]
                .as_materialized_series()
                .i64()
                .unwrap()
                .get(0),
            Some(1)
        );
        assert_eq!(
            batches[0].columns()[5].str_value(0).unwrap(),
            "DaftError::External [write_kafka] delivery failed"
        );
    }

    #[tokio::test]
    async fn close_flushes_and_returns_accounting_batch() {
        let mut writer = KafkaWriter::new(kafka_info(25), FakeProducer::succeeding(), 7);

        let batches = writer.close().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(
            batches[0].columns()[0]
                .as_materialized_series()
                .i64()
                .unwrap()
                .get(0),
            Some(7)
        );
        assert_eq!(writer.bytes_written(), 0);
        assert_eq!(writer.bytes_per_file(), vec![0]);
    }

    #[tokio::test]
    async fn close_surfaces_flush_error() {
        let producer = FakeProducer::with_flush_result(Err(delivery_error("flush failed")));
        let mut writer = KafkaWriter::new(kafka_info(25), producer, 7);

        let err = writer.close().await.unwrap_err();

        assert!(err.to_string().contains("[write_kafka] flush failed"));
    }
}
