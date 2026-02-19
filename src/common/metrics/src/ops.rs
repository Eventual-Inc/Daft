use std::{collections::HashMap, fmt::Display, sync::Arc};

use bincode::{Decode, Encode};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};

use crate::{ATTR_NODE_ID, ATTR_NODE_TYPE, NodeID};

#[derive(Clone, Debug, Default, Serialize, Deserialize, Encode, Decode)]
pub enum NodeType {
    // Sources
    // Produces MicroPartitions, never consumes
    #[default] // For testing purposes
    EmptyScan,
    GlobScan,
    InMemoryScan,
    ScanTask,

    // Intermediate Ops
    // Consumes a MicroPartition and immediately produces a resulting one. Little internal state
    CrossJoin,
    DistributedActorPoolProject,
    Explode,
    Filter,
    InnerHashJoinProbe,
    IntoBatches,
    Project,
    Sample,
    UDFProject,
    Unpivot,
    VLLMProject,

    // Blocking Sinks
    // Consumes all input MicroPartitions before producing 1-N outputs
    Aggregate,
    CommitWrite,
    JoinCollect,
    Dedup,
    GroupByAgg,
    HashJoinBuild,
    IntoPartitions,
    Pivot,
    Repartition,
    Sort,
    TopN,
    Window,
    Write,

    // Streaming Sinks
    // Both consumes and produces MicroPartitions at arbitrary intervals
    // For example, limit cuts off early.
    AntiSemiHashJoinProbe,
    AsyncUDFProject,
    Concat,
    Limit,
    MonotonicallyIncreasingId,
    OuterHashJoinProbe,
    SortMergeJoinProbe,

    // Specific to distributed only
    DistributedHashJoin,
    BroadcastJoin,
    SortMergeJoin,
}

impl Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Encode, Decode)]
pub enum NodeCategory {
    #[default] // For testing purposes
    Intermediate,
    Source,
    StreamingSink,
    BlockingSink,
}

impl Display for NodeCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Contains information about the node such as name, id, and the plan_id
#[derive(Clone, Debug, Default, Serialize, Deserialize, Encode, Decode)]
pub struct NodeInfo {
    pub name: Arc<str>,
    pub id: NodeID,
    #[allow(dead_code)]
    pub node_type: NodeType,
    pub node_category: NodeCategory,
    pub context: HashMap<String, String>,
}

impl NodeInfo {
    pub fn to_key_values(&self) -> Vec<KeyValue> {
        vec![
            KeyValue::new(ATTR_NODE_ID, self.id.to_string()),
            KeyValue::new(ATTR_NODE_TYPE, self.node_type.to_string()),
        ]
    }
}
