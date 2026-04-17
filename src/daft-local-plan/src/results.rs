use std::sync::Arc;

use arrow_array::{
    ArrayRef,
    builder::{
        DurationMillisecondBuilder, Float64Builder, LargeStringBuilder, MapBuilder, StructBuilder,
        UInt64Builder,
    },
};
use common_error::{DaftError, DaftResult};
use common_metrics::{QueryID, StatSnapshot, ops::NodeInfo, snapshot::StatSnapshotImpl};
use daft_core::prelude::{DataType, Field, Schema, TimeUnit};
use daft_recordbatch::RecordBatch;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SentinelRecord(#[serde(with = "serde_bytes")] Vec<u8>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sentinels(Vec<Option<SentinelRecord>>);

impl From<Vec<Option<RecordBatch>>> for Sentinels {
    fn from(batches: Vec<Option<RecordBatch>>) -> Self {
        Self(
            batches
                .into_iter()
                .map(|rb| {
                    rb.map(|r| {
                        SentinelRecord(
                            r.to_ipc_stream()
                                .expect("Failed to encode sentinel RecordBatch to IPC stream"),
                        )
                    })
                })
                .collect(),
        )
    }
}

impl Sentinels {
    pub fn into_record_batches(self) -> Vec<Option<RecordBatch>> {
        self.0
            .into_iter()
            .map(|s| {
                s.map(|sr| {
                    RecordBatch::from_ipc_stream(&sr.0)
                        .expect("Failed to decode sentinel RecordBatch from IPC stream")
                })
            })
            .collect()
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::legacy())
            .expect("Failed to encode Sentinels")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        let (sentinels, _): (Self, usize) =
            bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
                .expect("Failed to decode Sentinels");
        sentinels
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub query_id: QueryID,
    pub query_plan: Option<serde_json::Value>,
    pub nodes: Vec<(Arc<NodeInfo>, StatSnapshot)>,
}

impl ExecutionStats {
    pub fn new(query_id: QueryID, mut nodes: Vec<(Arc<NodeInfo>, StatSnapshot)>) -> Self {
        nodes.sort_by_key(|(node_info, _)| node_info.id);
        Self {
            query_id,
            query_plan: None,
            nodes,
        }
    }

    pub fn with_query_plan(mut self, query_plan: serde_json::Value) -> Self {
        self.query_plan = Some(query_plan);
        self
    }

    /// Encode the ExecutionStats into a binary format for transmission to scheduler
    pub fn encode(&self) -> Vec<u8> {
        bincode::encode_to_vec(&self.nodes, bincode::config::legacy())
            .expect("Failed to encode ExecutionStats")
    }

    /// Decode the ExecutionStats from a binary format received from scheduler
    pub fn decode(bytes: &[u8]) -> Self {
        let (nodes, _): (Vec<(Arc<NodeInfo>, StatSnapshot)>, usize) =
            bincode::decode_from_slice(bytes, bincode::config::legacy())
                .map_err(|e| {
                    DaftError::InternalError(format!("Failed to decode ExecutionStats: {e}"))
                })
                .unwrap();
        Self {
            query_id: "".into(),
            query_plan: None,
            nodes,
        }
    }

    /// Convert the ExecutionStats into a RecordBatch for visualization
    pub fn to_recordbatch(&self) -> DaftResult<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt64),
            Field::new("name", DataType::Utf8),
            Field::new("type", DataType::Utf8),
            Field::new("category", DataType::Utf8),
            Field::new("duration", DataType::Duration(TimeUnit::Milliseconds)),
            Field::new(
                "stats",
                DataType::Map {
                    key: Box::new(DataType::Utf8),
                    value: Box::new(DataType::Struct(vec![
                        Field::new("value", DataType::Float64),
                        Field::new("unit", DataType::Utf8),
                    ])),
                },
            ),
        ]);

        let mut ids = UInt64Builder::new();
        let mut names = LargeStringBuilder::new();
        let mut types = LargeStringBuilder::new();
        let mut categories = LargeStringBuilder::new();
        let mut duration_values = DurationMillisecondBuilder::new();
        let stats_values = StructBuilder::from_fields(
            vec![
                arrow_schema::Field::new("value", arrow_schema::DataType::Float64, false),
                arrow_schema::Field::new("unit", arrow_schema::DataType::LargeUtf8, true),
            ],
            self.nodes.len(),
        );
        let mut stats = MapBuilder::new(None, LargeStringBuilder::new(), stats_values);

        for (node_info, stat_snapshot) in &self.nodes {
            ids.append_value(node_info.id as u64);
            names.append_value(&node_info.name);
            types.append_value(node_info.node_type.to_string());
            categories.append_value(node_info.node_category.to_string());
            duration_values.append_value((stat_snapshot.duration_us() / 1000) as i64);
            for (name, value) in stat_snapshot.to_stats() {
                stats.keys().append_value(name);
                let values = stats.values();
                let (value, unit) = value.into_f64_and_unit();
                values
                    .field_builder::<Float64Builder>(0)
                    .unwrap()
                    .append_value(value);
                values
                    .field_builder::<LargeStringBuilder>(1)
                    .unwrap()
                    .append_option(unit);
                values.append(true);
            }
            stats.append(true)?;
        }

        RecordBatch::from_arrow(
            schema,
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(names.finish()) as ArrayRef,
                Arc::new(types.finish()) as ArrayRef,
                Arc::new(categories.finish()) as ArrayRef,
                Arc::new(duration_values.finish()) as ArrayRef,
                Arc::new(stats.finish()) as ArrayRef,
            ],
        )
    }
}
