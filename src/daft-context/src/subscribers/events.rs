use std::{collections::HashMap, sync::Arc};

use common_metrics::{
    NodeID, QueryID, Stats,
    ops::{NodeCategory, NodeInfo, NodeType},
};

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

#[derive(Debug)]
pub struct OperatorStartEvent {
    pub header: EventHeader,
    pub operator: Arc<OperatorMeta>,
}

#[derive(Debug)]
pub struct OperatorEndEvent {
    pub header: EventHeader,
    pub operator: Arc<OperatorMeta>,
}

#[derive(Debug)]
pub struct StatsEvent {
    pub header: EventHeader,
    pub stats: Arc<Vec<(NodeID, Stats)>>,
}
