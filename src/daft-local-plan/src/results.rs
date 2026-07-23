use std::{collections::HashMap, sync::Arc};

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

const PROFILE_WIRE_V2_MAGIC: &[u8] = b"DAFTSTATS2";

#[derive(Debug, Clone, Default, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ProfilePartitionStats {
    pub count: u64,
    pub total_rows: u64,
    pub max_rows: u64,
    pub total_bytes: u64,
    pub max_bytes: u64,
}

impl ProfilePartitionStats {
    pub fn record(&mut self, rows: u64, bytes: u64) {
        self.count += 1;
        self.total_rows += rows;
        self.max_rows = self.max_rows.max(rows);
        self.total_bytes += bytes;
        self.max_bytes = self.max_bytes.max(bytes);
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ScanPushdownStats {
    pub filter_requested: bool,
    pub filter_applied: bool,
    pub projection_requested: bool,
    pub projection_applied: bool,
}

impl ScanPushdownStats {
    pub fn merge(&mut self, other: &Self) {
        self.filter_requested |= other.filter_requested;
        self.filter_applied &= other.filter_applied;
        self.projection_requested |= other.projection_requested;
        self.projection_applied &= other.projection_applied;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ProfileTelemetry {
    pub peak_process_rss_bytes: Option<u64>,
    pub spilled_bytes: Option<u64>,
    pub partition_stats: HashMap<usize, ProfilePartitionStats>,
    pub scan_pushdowns: HashMap<usize, ScanPushdownStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub query_id: QueryID,
    pub query_plan: Option<serde_json::Value>,
    pub nodes: Vec<(Arc<NodeInfo>, StatSnapshot)>,
    /// Files skipped due to `ignore_corrupt_files=True`: (path, reason, partial).
    pub skipped_corrupt_files: Vec<(String, String, bool)>,
    #[serde(default)]
    pub profile_telemetry: ProfileTelemetry,
}

impl ExecutionStats {
    pub fn new(query_id: QueryID, mut nodes: Vec<(Arc<NodeInfo>, StatSnapshot)>) -> Self {
        nodes.sort_by_key(|(node_info, _)| node_info.id);
        Self {
            query_id,
            query_plan: None,
            nodes,
            skipped_corrupt_files: vec![],
            profile_telemetry: ProfileTelemetry::default(),
        }
    }

    pub fn with_skipped_corrupt_files(
        mut self,
        skipped_corrupt_files: Vec<(String, String, bool)>,
    ) -> Self {
        let mut seen = std::collections::HashSet::new();
        self.skipped_corrupt_files = skipped_corrupt_files
            .into_iter()
            .filter(|(path, _, _)| seen.insert(path.clone()))
            .collect();
        self
    }

    pub fn with_query_plan(mut self, query_plan: serde_json::Value) -> Self {
        self.query_plan = Some(query_plan);
        self
    }

    pub fn with_profile_telemetry(mut self, profile_telemetry: ProfileTelemetry) -> Self {
        self.profile_telemetry = profile_telemetry;
        self
    }

    /// Encode the ExecutionStats into a binary format for transmission to scheduler
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = PROFILE_WIRE_V2_MAGIC.to_vec();
        encoded.extend(
            bincode::encode_to_vec(
                (
                    &self.nodes,
                    &self.skipped_corrupt_files,
                    &self.profile_telemetry,
                ),
                bincode::config::legacy(),
            )
            .expect("Failed to encode ExecutionStats"),
        );
        encoded
    }

    /// Decode the ExecutionStats from a binary format received from scheduler
    pub fn decode(bytes: &[u8]) -> Self {
        if let Some(payload) = bytes.strip_prefix(PROFILE_WIRE_V2_MAGIC) {
            type DecodedV2 = (
                Vec<(Arc<NodeInfo>, StatSnapshot)>,
                Vec<(String, String, bool)>,
                ProfileTelemetry,
            );
            let ((nodes, skipped_corrupt_files, profile_telemetry), _): (DecodedV2, usize) =
                bincode::decode_from_slice(payload, bincode::config::legacy())
                    .map_err(|e| {
                        DaftError::InternalError(format!("Failed to decode ExecutionStats: {e}"))
                    })
                    .unwrap();
            return Self {
                query_id: "".into(),
                query_plan: None,
                nodes,
                skipped_corrupt_files,
                profile_telemetry,
            };
        }
        type Decoded = (
            Vec<(Arc<NodeInfo>, StatSnapshot)>,
            Vec<(String, String, bool)>,
        );
        let ((nodes, skipped_corrupt_files), _): (Decoded, usize) =
            bincode::decode_from_slice(bytes, bincode::config::legacy())
                .map_err(|e| {
                    DaftError::InternalError(format!("Failed to decode ExecutionStats: {e}"))
                })
                .unwrap();
        Self {
            query_id: "".into(),
            query_plan: None,
            nodes,
            skipped_corrupt_files,
            profile_telemetry: ProfileTelemetry::default(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_telemetry_roundtrips_in_v2_wire_format() {
        let mut telemetry = ProfileTelemetry {
            peak_process_rss_bytes: Some(1234),
            spilled_bytes: Some(5678),
            ..Default::default()
        };
        telemetry
            .partition_stats
            .entry(7)
            .or_default()
            .record(100, 200);
        let stats = ExecutionStats::new("query".into(), vec![]).with_profile_telemetry(telemetry);

        let decoded = ExecutionStats::decode(&stats.encode());

        assert_eq!(decoded.profile_telemetry.peak_process_rss_bytes, Some(1234));
        assert_eq!(decoded.profile_telemetry.spilled_bytes, Some(5678));
        assert_eq!(decoded.profile_telemetry.partition_stats[&7].max_rows, 100);
    }

    #[test]
    fn legacy_wire_format_decodes_with_empty_profile_telemetry() {
        let nodes: Vec<(Arc<NodeInfo>, StatSnapshot)> = vec![];
        let skipped: Vec<(String, String, bool)> = vec![];
        let encoded =
            bincode::encode_to_vec((&nodes, &skipped), bincode::config::legacy()).unwrap();

        let decoded = ExecutionStats::decode(&encoded);

        assert_eq!(decoded.profile_telemetry.peak_process_rss_bytes, None);
        assert!(decoded.profile_telemetry.partition_stats.is_empty());
    }
}
