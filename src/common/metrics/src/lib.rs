use std::time::Duration;

use indicatif::{HumanBytes, HumanCount, HumanDuration, HumanFloatCount};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalPhysicalNodeMetrics {
    // Task context
    pub plan_id: u16,
    pub stage_id: u16,
    pub task_id: u32,
    pub logical_node_id: u32,

    // Required for performing node-type aware metrics aggregation
    pub local_physical_node_type: String,
    pub distributed_physical_node_type: String, // This is available as node_name in the context
}

pub type StatSnapshot = SmallVec<[(String, Stat); 3]>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stat {
    // Integer Representations
    Count(u64),
    Bytes(u64),
    // Base Types
    Float(f64),
    Duration(Duration),
}

impl std::fmt::Display for Stat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count(value) => write!(f, "{}", HumanCount(*value)),
            Self::Bytes(value) => write!(f, "{}", HumanBytes(*value)),
            Self::Float(value) => write!(f, "{}", HumanFloatCount(*value)),
            Self::Duration(value) => write!(f, "{}", HumanDuration(*value)),
        }
    }
}

pub type RpcPayload = (LocalPhysicalNodeMetrics, StatSnapshot);
