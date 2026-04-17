use std::{collections::HashMap, sync::Arc};

use common_metrics::{
    NodeID, QueryID, QueryPlan, Stats,
    ops::{NodeCategory, NodeInfo, NodeType},
};
use daft_micropartition::MicroPartitionRef;

use super::{QueryMetadata, QueryResult};

#[derive(Debug, Clone)]
pub enum Event {
    QueryStart(QueryStartEvent),
    QueryHeartbeat(QueryHeartbeatEvent),
    QueryEnd(QueryEndEvent),
    OptimizationStart(OptimizationStartEvent),
    OptimizationComplete(OptimizationCompleteEvent),
    ExecStart(ExecStartEvent),
    ExecEnd(ExecEndEvent),
    OperatorStart(OperatorStartEvent),
    OperatorEnd(OperatorEndEvent),
    Stats(StatsEvent),
    ProcessStats(ProcessStatsEvent),
    ResultOut(ResultOutEvent),
}

#[derive(Debug, Clone)]
pub struct EventHeader {
    pub query_id: QueryID,
    pub timestamp_epoch_secs: f64,
}

#[derive(Debug, Clone)]
pub struct OperatorMeta {
    pub node_id: NodeID,
    pub name: Arc<str>,
    pub node_type: NodeType,
    pub node_category: NodeCategory,
    pub origin_node_id: NodeID,
    pub node_phase: Option<String>,
    pub context: HashMap<String, String>,
}

impl OperatorMeta {
    // Placeholder until we can get more data from distributed
    // on_operator_start and on_operator_end calls
    pub fn from_id(node_id: NodeID) -> Self {
        Self {
            node_id,
            name: Arc::from("unknown"),
            node_type: NodeType::default(),
            node_category: NodeCategory::default(),
            origin_node_id: node_id,
            node_phase: None,
            context: HashMap::new(),
        }
    }
}

impl From<&NodeInfo> for OperatorMeta {
    fn from(info: &NodeInfo) -> Self {
        Self {
            node_id: info.id,
            name: info.name.clone(),
            node_type: info.node_type.clone(),
            node_category: info.node_category.clone(),
            origin_node_id: info.node_origin_id,
            node_phase: info.node_phase.clone(),
            context: info.context.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperatorStartEvent {
    pub header: EventHeader,
    pub operator: Arc<OperatorMeta>,
}

#[derive(Debug, Clone)]
pub struct OperatorEndEvent {
    pub header: EventHeader,
    pub operator: Arc<OperatorMeta>,
}

#[derive(Debug, Clone)]
pub struct StatsEvent {
    pub header: EventHeader,
    pub stats: Arc<Vec<(NodeID, Stats)>>,
}

#[derive(Debug, Clone)]
pub struct ProcessStatsEvent {
    pub header: EventHeader,
    pub stats: Stats,
}

#[derive(Debug, Clone)]
pub struct QueryStartEvent {
    pub header: EventHeader,
    pub metadata: Arc<QueryMetadata>,
}

#[derive(Debug, Clone)]
pub struct QueryHeartbeatEvent {
    pub header: EventHeader,
}

#[derive(Debug, Clone)]
pub struct QueryEndEvent {
    pub header: EventHeader,
    pub result: QueryResult,
    pub duration_ms: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct OptimizationStartEvent {
    pub header: EventHeader,
}

#[derive(Debug, Clone)]
pub struct OptimizationCompleteEvent {
    pub header: EventHeader,
    pub optimized_plan: QueryPlan,
}

#[derive(Debug, Clone)]
pub struct ExecStartEvent {
    pub header: EventHeader,
    pub physical_plan: QueryPlan,
}

#[derive(Debug, Clone)]
pub struct ExecEndEvent {
    pub header: EventHeader,
    pub duration_ms: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct ResultOutEvent {
    pub header: EventHeader,
    pub num_rows: u64,
    // needed by the dashboard subscriber
    pub data: Option<MicroPartitionRef>,
}
