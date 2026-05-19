use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow_array::{
    ArrayRef,
    builder::{
        DurationMillisecondBuilder, Float64Builder, LargeStringBuilder, MapBuilder, StructBuilder,
        UInt64Builder,
    },
};
use common_error::{DaftError, DaftResult};
use common_metrics::{
    DURATION_KEY, QueryID, Stat, StatSnapshot, ops::NodeInfo, snapshot::StatSnapshotImpl,
};
use daft_core::prelude::{DataType, Field, Schema, TimeUnit};
use daft_recordbatch::RecordBatch;
use serde::{Deserialize, Serialize};

const EXECUTION_TIME_TOTAL_KEY: &str = "execution_time_total";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub query_id: QueryID,
    pub query_plan: Option<serde_json::Value>,
    pub nodes: Vec<(Arc<NodeInfo>, StatSnapshot)>,
    pub wall_times: HashMap<u32, u64>,
}

impl ExecutionStats {
    pub fn new(query_id: QueryID, mut nodes: Vec<(Arc<NodeInfo>, StatSnapshot)>) -> Self {
        nodes.sort_by_key(|(node_info, _)| node_info.id);
        Self {
            query_id,
            query_plan: None,
            nodes,
            wall_times: HashMap::new(),
        }
    }

    pub fn with_query_plan(mut self, query_plan: serde_json::Value) -> Self {
        self.query_plan = Some(query_plan);
        self
    }

    pub fn with_wall_times(mut self, wall_times: HashMap<u32, u64>) -> Self {
        self.wall_times = wall_times;
        self
    }

    /// Encode the ExecutionStats into a binary format for transmission to scheduler
    ///
    /// Only `nodes` are serialized: runner-level fields such as distributed
    /// lifecycle wall time are attached later by the scheduler's final
    /// `StatisticsManager::export_metrics()` path.
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
            wall_times: HashMap::new(),
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

            // The top-level duration column is the user-facing operator duration. Use
            // runner-provided wall-clock lifecycle time when available; otherwise fall back
            // to the accumulated execution duration from the snapshot for runners that do
            // not yet report lifecycle wall time.
            let execution_time_total = stat_snapshot.duration_us();
            let node_id = node_info.id as u32;
            let duration_us = self
                .wall_times
                .get(&node_id)
                .copied()
                .unwrap_or(execution_time_total);
            duration_values.append_value((duration_us / 1000) as i64);
            for (name, value) in stat_snapshot.to_stats() {
                // duration is promoted to the top-level column above.
                // keep the stats map from carrying a second, differently-defined duration.
                // the accumulated snapshot duration is exposed as `execution_time_total` instead.
                if name.as_ref() == DURATION_KEY {
                    continue;
                }
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

            // preserve the accumulated execution duration that snapshots report. In
            // distributed runners this can exceed wall-clock duration because it sums work
            // across concurrent tasks/work units.
            stats.keys().append_value(EXECUTION_TIME_TOTAL_KEY);
            let values = stats.values();
            let (value, unit) =
                Stat::Duration(Duration::from_micros(execution_time_total)).into_f64_and_unit();
            values
                .field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(value);
            values
                .field_builder::<LargeStringBuilder>(1)
                .unwrap()
                .append_option(unit);
            values.append(true);

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
