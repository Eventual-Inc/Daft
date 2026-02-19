use std::sync::Arc;

use arrow_array::{
    ArrayRef,
    builder::{
        DurationMicrosecondBuilder, Float64Builder, LargeStringBuilder, MapBuilder, StructBuilder,
        UInt64Builder,
    },
};
use common_error::{DaftError, DaftResult};
use common_metrics::{
    CPU_US_KEY, QueryID, Stat, StatSnapshot, ops::NodeInfo, snapshot::StatSnapshotImpl,
};
use daft_core::prelude::{DataType, Field, Schema, TimeUnit};
use daft_recordbatch::RecordBatch;

#[derive(Debug, Clone)]
pub struct ExecutionMetadata {
    pub query_id: QueryID,
    pub query_plan: Option<serde_json::Value>,
    pub nodes: Vec<(Arc<NodeInfo>, StatSnapshot)>,
}

impl ExecutionMetadata {
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

    /// Encode the ExecutionMetadata into a binary format for transmission to scheduler
    pub fn encode(&self) -> Vec<u8> {
        bincode::encode_to_vec(&self.nodes, bincode::config::legacy())
            .expect("Failed to encode ExecutionMetadata")
    }

    /// Decode the ExecutionMetadata from a binary format received from scheduler
    pub fn decode(bytes: &[u8]) -> Self {
        let (nodes, _): (Vec<(Arc<NodeInfo>, StatSnapshot)>, usize) =
            bincode::decode_from_slice(bytes, bincode::config::legacy())
                .map_err(|e| {
                    DaftError::InternalError(format!("Failed to decode ExecutionMetadata: {e}"))
                })
                .unwrap();
        Self {
            query_id: "".into(),
            query_plan: None,
            nodes,
        }
    }

    /// Convert the ExecutionMetadata into a RecordBatch for visualization
    pub fn to_recordbatch(&self) -> DaftResult<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt64),
            Field::new("name", DataType::Utf8),
            Field::new("type", DataType::Utf8),
            Field::new("category", DataType::Utf8),
            Field::new("cpu us", DataType::Duration(TimeUnit::Microseconds)),
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
        let mut cpu_us_values = DurationMicrosecondBuilder::new();
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
            for (name, value) in stat_snapshot.to_stats() {
                // Note: Always expect one stat for duration by the execution engine
                // TODO: Add checks just in case
                if name.as_ref() == CPU_US_KEY {
                    let Stat::Duration(cpu_us) = value else {
                        panic!("cpu us is always a Duration in stats");
                    };
                    cpu_us_values.append_value(cpu_us.as_micros() as i64);
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
            stats.append(true)?;
        }

        RecordBatch::from_arrow(
            schema,
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(names.finish()) as ArrayRef,
                Arc::new(types.finish()) as ArrayRef,
                Arc::new(categories.finish()) as ArrayRef,
                Arc::new(cpu_us_values.finish()) as ArrayRef,
                Arc::new(stats.finish()) as ArrayRef,
            ],
        )
    }
}
