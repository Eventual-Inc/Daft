use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormatConfig;
use common_scan_info::{PartitionField, Pushdowns, ScanOperator, ScanTaskLikeRef};
use daft_schema::{dtype::DataType, field::Field, schema::Schema};
use rdkafka::{
    ClientConfig,
    consumer::{BaseConsumer, Consumer},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
};

use crate::{DataSource, ScanTask, storage_config::StorageConfig};

fn resolve_time_offset(
    consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
    ts_ms: i64,
    timeout: Timeout,
) -> DaftResult<i64> {
    let mut tpl = TopicPartitionList::new();

    tpl.add_partition_offset(topic, partition, Offset::Offset(ts_ms))
        .map_err(|e| DaftError::External(Box::new(e)))?;

    let out = consumer
        .offsets_for_times(tpl, timeout)
        .map_err(|e| DaftError::External(Box::new(e)))?;

    let mut resolved: Option<i64> = None;

    for elem in out.elements() {
        if elem.topic() == topic && elem.partition() == partition {
            match elem.offset() {
                Offset::Offset(resolved_offset) => resolved = Some(resolved_offset),
                _ => resolved = None,
            }
            break;
        }
    }

    resolved
        .ok_or_else(|| DaftError::ValueError("Failed to resolve timestamp to offset".to_string()))
}

#[allow(clippy::too_many_arguments)]
fn resolve_bound(
    consumer: &BaseConsumer,
    kind: &str,
    ts_ms: Option<i64>,
    offsets_by_topic: Option<&BTreeMap<String, BTreeMap<i32, i64>>>,
    low: i64,
    high: i64,
    topic: &str,
    partition: i32,
    timeout: Timeout,
) -> DaftResult<i64> {
    match kind {
        "earliest" => Ok(low),
        "latest" => Ok(high),
        "timestamp_ms" => {
            let ts = ts_ms
                .ok_or_else(|| DaftError::ValueError("timestamp_ms is required".to_string()))?;
            let resolved_offset = resolve_time_offset(consumer, topic, partition, ts, timeout)?;
            Ok(resolved_offset.max(low).min(high))
        }
        "topic_partition_offsets" => {
            let by_topic = offsets_by_topic.ok_or_else(|| {
                DaftError::ValueError("topic/partition offset map is required".to_string())
            })?;
            let per_topic = by_topic.get(topic).ok_or_else(|| {
                DaftError::ValueError(format!("missing offsets for topic {topic}"))
            })?;
            let configured_offset = per_topic.get(&partition).ok_or_else(|| {
                DaftError::ValueError(format!(
                    "missing offset for partition {partition} of topic {topic}"
                ))
            })?;
            if *configured_offset < 0 {
                return Err(DaftError::ValueError(
                    "partition offsets must be >= 0".to_string(),
                ));
            }
            Ok((*configured_offset).max(low).min(high))
        }
        other => Err(DaftError::ValueError(format!(
            "unsupported bound kind: {other}"
        ))),
    }
}

#[derive(Debug)]
pub struct KafkaScanOperator {
    bootstrap_servers: String,
    group_id: String,
    topics: Vec<String>,
    start_kind: String,
    start_timestamp_ms: Option<i64>,
    start_topic_partition_offsets: Option<BTreeMap<String, BTreeMap<i32, i64>>>,
    end_kind: String,
    end_timestamp_ms: Option<i64>,
    end_topic_partition_offsets: Option<BTreeMap<String, BTreeMap<i32, i64>>>,
    partitions: Option<Vec<i32>>,
    kafka_client_config: Option<BTreeMap<String, String>>,
    timeout_ms: u64,
    schema: daft_schema::schema::SchemaRef,
}

impl KafkaScanOperator {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bootstrap_servers: String,
        group_id: String,
        topics: Vec<String>,
        start_kind: String,
        start_timestamp_ms: Option<i64>,
        start_topic_partition_offsets: Option<BTreeMap<String, BTreeMap<i32, i64>>>,
        end_kind: String,
        end_timestamp_ms: Option<i64>,
        end_topic_partition_offsets: Option<BTreeMap<String, BTreeMap<i32, i64>>>,
        partitions: Option<Vec<i32>>,
        kafka_client_config: Option<BTreeMap<String, String>>,
        timeout_ms: u64,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("topic", DataType::Utf8),
            Field::new("partition", DataType::Int32),
            Field::new("offset", DataType::Int64),
            Field::new("timestamp_ms", DataType::Int64),
            Field::new("key", DataType::Binary),
            Field::new("value", DataType::Binary),
        ]));

        Self {
            bootstrap_servers,
            group_id,
            topics,
            start_kind,
            start_timestamp_ms,
            start_topic_partition_offsets,
            end_kind,
            end_timestamp_ms,
            end_topic_partition_offsets,
            partitions,
            kafka_client_config,
            timeout_ms,
            schema,
        }
    }
}

impl ScanOperator for KafkaScanOperator {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "kafka"
    }

    fn schema(&self) -> daft_schema::schema::SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
    }

    fn file_path_column(&self) -> Option<&str> {
        None
    }

    fn generated_fields(&self) -> Option<daft_schema::schema::SchemaRef> {
        None
    }

    fn can_absorb_filter(&self) -> bool {
        false
    }

    fn can_absorb_select(&self) -> bool {
        false
    }

    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "KafkaScanOperator(topics={:?}, start_kind={}, end_kind={})",
            self.topics, self.start_kind, self.end_kind
        )]
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        let mut cfg = ClientConfig::new();
        if let Some(extra) = &self.kafka_client_config {
            for (k, v) in extra {
                cfg.set(k, v);
            }
        }

        let consumer: BaseConsumer = cfg
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", &self.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .create()
            .map_err(|e| DaftError::External(Box::new(e)))?;

        let partition_filter: Option<HashSet<i32>> = self
            .partitions
            .as_ref()
            .map(|ps| ps.iter().copied().collect());
        let mut topic_partitions: Vec<(String, i32)> = Vec::new();
        for topic in &self.topics {
            let metadata = consumer
                .fetch_metadata(Some(topic.as_str()), Duration::from_millis(self.timeout_ms))
                .map_err(|e| DaftError::External(Box::new(e)))?;
            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|topic_metadata| topic_metadata.name() == topic)
                .ok_or_else(|| DaftError::ValueError(format!("topic not found: {topic}")))?;
            for partition_metadata in topic_metadata.partitions() {
                let partition_id = partition_metadata.id();
                if partition_filter
                    .as_ref()
                    .is_some_and(|filter| !filter.contains(&partition_id))
                {
                    continue;
                }
                topic_partitions.push((topic.clone(), partition_id));
            }
        }

        let timeout = Timeout::After(Duration::from_millis(self.timeout_ms));
        if self.start_kind == "topic_partition_offsets"
            && self.start_topic_partition_offsets.is_none()
        {
            return Err(DaftError::ValueError(
                "start_topic_partition_offsets is required when start_kind=topic_partition_offsets"
                    .to_string(),
            ));
        }
        if self.end_kind == "topic_partition_offsets" && self.end_topic_partition_offsets.is_none()
        {
            return Err(DaftError::ValueError(
                "end_topic_partition_offsets is required when end_kind=topic_partition_offsets"
                    .to_string(),
            ));
        }

        let storage_config: Arc<StorageConfig> = Arc::new(StorageConfig::default());
        let file_format_config = Arc::new(FileFormatConfig::PythonFunction {
            source_type: Some("Kafka".to_string()),
            module_name: None,
            function_name: None,
        });

        let mut scan_tasks: Vec<ScanTaskLikeRef> = Vec::with_capacity(topic_partitions.len());
        for (topic, partition) in topic_partitions {
            let (low, high) = consumer
                .fetch_watermarks(topic.as_str(), partition, timeout)
                .map_err(|e| DaftError::External(Box::new(e)))?;

            let start_offset = resolve_bound(
                &consumer,
                self.start_kind.as_str(),
                self.start_timestamp_ms,
                self.start_topic_partition_offsets.as_ref(),
                low,
                high,
                topic.as_str(),
                partition,
                timeout,
            )?;

            let end_offset = resolve_bound(
                &consumer,
                self.end_kind.as_str(),
                self.end_timestamp_ms,
                self.end_topic_partition_offsets.as_ref(),
                low,
                high,
                topic.as_str(),
                partition,
                timeout,
            )?;

            if end_offset < start_offset {
                return Err(DaftError::ValueError("end must be >= start".to_string()));
            }

            if start_offset == end_offset {
                continue;
            }

            let path = format!(
                "kafka://{topic}/{partition}?start={}&end={}",
                start_offset, end_offset
            );
            let source = DataSource::Kafka {
                path,
                bootstrap_servers: self.bootstrap_servers.clone(),
                group_id: self.group_id.clone(),
                topic,
                partition,
                start_offset,
                end_offset,
                kafka_client_config: self.kafka_client_config.clone(),
                timeout_ms: self.timeout_ms,
            };

            scan_tasks.push(Arc::new(ScanTask::new(
                vec![source],
                file_format_config.clone(),
                self.schema.clone(),
                storage_config.clone(),
                pushdowns.clone(),
                None,
            )) as ScanTaskLikeRef);
        }

        Ok(scan_tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_consumer() -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", "daft-kafka-scan-test")
            .create()
            .unwrap()
    }

    #[test]
    fn test_resolve_bound_earliest_latest() {
        let consumer = dummy_consumer();
        assert_eq!(
            resolve_bound(
                &consumer,
                "earliest",
                None,
                None,
                10,
                20,
                "topic",
                0,
                Timeout::Never
            )
            .unwrap(),
            10
        );
        assert_eq!(
            resolve_bound(
                &consumer,
                "latest",
                None,
                None,
                10,
                20,
                "topic",
                0,
                Timeout::Never
            )
            .unwrap(),
            20
        );
    }

    #[test]
    fn test_resolve_bound_timestamp_requires_value() {
        let consumer = dummy_consumer();
        let err = resolve_bound(
            &consumer,
            "timestamp_ms",
            None,
            None,
            0,
            10,
            "topic",
            0,
            Timeout::Never,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            DaftError::ValueError(ref msg) if msg.contains("timestamp_ms is required")
        ));
    }

    #[test]
    fn test_resolve_bound_topic_partition_offsets_clamps_and_errors() {
        let consumer = dummy_consumer();

        let mut by_topic: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
        let mut per_topic: BTreeMap<i32, i64> = BTreeMap::new();
        per_topic.insert(0, 5);
        per_topic.insert(1, 50);
        by_topic.insert("topic".to_string(), per_topic);

        assert_eq!(
            resolve_bound(
                &consumer,
                "topic_partition_offsets",
                None,
                Some(&by_topic),
                10,
                20,
                "topic",
                0,
                Timeout::Never
            )
            .unwrap(),
            10
        );
        assert_eq!(
            resolve_bound(
                &consumer,
                "topic_partition_offsets",
                None,
                Some(&by_topic),
                10,
                20,
                "topic",
                1,
                Timeout::Never
            )
            .unwrap(),
            20
        );

        let err = resolve_bound(
            &consumer,
            "topic_partition_offsets",
            None,
            None,
            0,
            10,
            "t",
            0,
            Timeout::Never,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            DaftError::ValueError(ref msg) if msg.contains("missing offsets for topic t")
        ));

        let mut by_topic_missing: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
        by_topic_missing.insert("other".to_string(), BTreeMap::new());
        let err = resolve_bound(
            &consumer,
            "topic_partition_offsets",
            None,
            Some(&by_topic_missing),
            0,
            10,
            "t",
            0,
            Timeout::Never,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            DaftError::ValueError(ref msg) if msg.contains("missing offsets for topic t")
        ));

        let mut by_topic_negative: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
        let mut per_topic_negative: BTreeMap<i32, i64> = BTreeMap::new();
        per_topic_negative.insert(0, -1);
        by_topic_negative.insert("topic".to_string(), per_topic_negative);
        let err = resolve_bound(
            &consumer,
            "topic_partition_offsets",
            None,
            Some(&by_topic_negative),
            0,
            10,
            "topic",
            0,
            Timeout::Never,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            DaftError::ValueError(ref msg) if msg.contains("partition offsets must be >= 0")
        ));
    }

    #[test]
    fn test_resolve_bound_unknown_kind() {
        let consumer = dummy_consumer();
        let err = resolve_bound(
            &consumer,
            "nope",
            None,
            None,
            0,
            10,
            "topic",
            0,
            Timeout::Never,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            DaftError::ValueError(ref msg) if msg.contains("unsupported bound kind: nope")
        ));
    }
}
