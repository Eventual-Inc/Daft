use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::{DataType, Field, Int64Array, IntoSeries, Schema, Utf8Array};
use daft_recordbatch::RecordBatch;

#[derive(Debug, Default, Clone)]
pub struct KafkaWriteAccounting {
    pub task_id: i64,
    pub messages_attempted: i64,
    pub messages_delivered: i64,
    pub messages_failed: i64,
    pub bytes_delivered: i64,
    pub first_error: Option<String>,
}

impl KafkaWriteAccounting {
    pub fn new(task_id: i64) -> Self {
        Self {
            task_id,
            ..Default::default()
        }
    }

    pub fn record_attempt(&mut self) {
        self.messages_attempted = self.messages_attempted.saturating_add(1);
    }

    pub fn record_delivery(&mut self, bytes: usize) {
        self.messages_delivered = self.messages_delivered.saturating_add(1);
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        self.bytes_delivered = self.bytes_delivered.saturating_add(bytes);
    }

    pub fn record_failure(&mut self, err: impl Into<String>) {
        self.messages_failed = self.messages_failed.saturating_add(1);
        if self.first_error.is_none() {
            self.first_error = Some(err.into());
        }
    }

    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("task_id", DataType::Int64),
            Field::new("messages_attempted", DataType::Int64),
            Field::new("messages_delivered", DataType::Int64),
            Field::new("messages_failed", DataType::Int64),
            Field::new("bytes_delivered", DataType::Int64),
            Field::new("first_error", DataType::Utf8),
        ]))
    }

    pub fn to_record_batch(&self) -> DaftResult<RecordBatch> {
        RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("task_id", vec![self.task_id]).into_series(),
            Int64Array::from_vec("messages_attempted", vec![self.messages_attempted]).into_series(),
            Int64Array::from_vec("messages_delivered", vec![self.messages_delivered]).into_series(),
            Int64Array::from_vec("messages_failed", vec![self.messages_failed]).into_series(),
            Int64Array::from_vec("bytes_delivered", vec![self.bytes_delivered]).into_series(),
            Utf8Array::from_iter("first_error", [self.first_error.as_deref()]).into_series(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::DataType;

    use super::*;

    #[test]
    fn counts_deliveries_and_failures_and_preserves_first_error() {
        let mut accounting = KafkaWriteAccounting::new(7);

        accounting.record_attempt();
        accounting.record_attempt();
        accounting.record_delivery(12);
        accounting.record_failure("first");
        accounting.record_failure("second");

        assert_eq!(accounting.task_id, 7);
        assert_eq!(accounting.messages_attempted, 2);
        assert_eq!(accounting.messages_delivered, 1);
        assert_eq!(accounting.messages_failed, 2);
        assert_eq!(accounting.bytes_delivered, 12);
        assert_eq!(accounting.first_error.as_deref(), Some("first"));
    }

    #[test]
    fn saturates_accounting_counters() {
        let mut accounting = KafkaWriteAccounting {
            messages_attempted: i64::MAX,
            messages_delivered: i64::MAX,
            messages_failed: i64::MAX,
            bytes_delivered: i64::MAX - 1,
            ..KafkaWriteAccounting::new(11)
        };

        accounting.record_attempt();
        accounting.record_delivery(usize::MAX);
        accounting.record_failure("overflow");

        assert_eq!(accounting.messages_attempted, i64::MAX);
        assert_eq!(accounting.messages_delivered, i64::MAX);
        assert_eq!(accounting.messages_failed, i64::MAX);
        assert_eq!(accounting.bytes_delivered, i64::MAX);
        assert_eq!(accounting.first_error.as_deref(), Some("overflow"));
    }

    #[test]
    fn builds_summary_record_batch_with_expected_schema_and_null_error() {
        let accounting = KafkaWriteAccounting::new(9);
        let batch = accounting.to_record_batch().unwrap();

        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch.schema.as_ref(),
            KafkaWriteAccounting::schema().as_ref()
        );
        let first_error = &batch.columns()[5];
        assert_eq!(first_error.name(), "first_error");
        assert_eq!(first_error.data_type(), &DataType::Utf8);
        assert!(!first_error.is_valid(0));
    }

    #[test]
    fn builds_summary_record_batch_with_error_value() {
        let mut accounting = KafkaWriteAccounting::new(10);
        accounting.record_failure("failed");
        let batch = accounting.to_record_batch().unwrap();

        assert_eq!(batch.len(), 1);
        assert_eq!(batch.columns()[5].str_value(0).unwrap(), "failed");
    }
}
