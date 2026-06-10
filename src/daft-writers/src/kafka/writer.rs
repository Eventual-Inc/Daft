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
    producer::{KafkaProducer, RdkafkaProducer},
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
            DaftError::External(
                format!("[write_kafka] failed to create Kafka producer: {err}").into(),
            )
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

    async fn write(&mut self, _data: Self::Input) -> DaftResult<WriteResult> {
        Err(DaftError::not_implemented(
            "[write_kafka] row projection is not wired into KafkaWriter yet",
        ))
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

    use daft_core::prelude::{DataType, Field, Schema};
    use daft_dsl::bound_col;
    use daft_logical_plan::sink_info::{KafkaKeyFormat, KafkaTopic, KafkaValueFormat};

    use super::*;
    use crate::kafka::producer::{FakeProducer, delivery_error};

    fn kafka_info(timeout_ms: u64) -> KafkaWriteInfo<BoundExpr> {
        KafkaWriteInfo {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: KafkaTopic::Static("topic".to_string()),
            value_col: BoundExpr::new_unchecked(bound_col(0, Field::new("value", DataType::Utf8))),
            key_col: None,
            headers_col: None,
            partition: None,
            timestamp_ms_col: None,
            value_format: KafkaValueFormat::Raw,
            key_format: KafkaKeyFormat::Raw,
            kafka_client_config: BTreeMap::new(),
            timeout_ms,
        }
    }

    #[tokio::test]
    async fn write_returns_not_implemented_until_row_projection_is_wired() {
        let mut writer = KafkaWriter::new(kafka_info(1), FakeProducer::succeeding(), 3);
        let result = writer
            .write(MicroPartition::empty(Some(Arc::new(Schema::empty()))))
            .await;
        let err = match result {
            Ok(_) => panic!("KafkaWriter::write should fail before row projection is wired"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("[write_kafka] row projection is not wired")
        );
        assert_eq!(writer.bytes_written(), 0);
        assert_eq!(writer.bytes_per_file(), vec![0]);
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
