use std::{collections::HashMap, fmt::Display, sync::Arc};

#[derive(Clone, Debug)]
pub enum NodeType {
    // Sources
    // Produces MicroPartitions, never consumes
    EmptyScan,
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

    // Blocking Sinks
    // Consumes all input MicroPartitions before producing 1-N outputs
    Aggregate,
    CommitWrite,
    CrossJoinCollect,
    Dedup,
    GroupByAgg,
    HashJoinBuild,
    IntoPartitions,
    Pivot,
    Repartition,
    Sort,
    TopN,
    WindowOrderByOnly,
    WindowPartitionAndDynamicFrame,
    WindowPartitionAndOrderBy,
    WindowPartitionOnly,
    Write,

    // Streaming Sinks
    // Both consumes and produces MicroPartitions at arbitrary intervals
    // For example, limit cuts off early.
    AntiSemiHashJoinProbe,
    Concat,
    Limit,
    MonotonicallyIncreasingId,
    OuterHashJoinProbe,
}

impl Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub enum NodeCategory {
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
#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub name: Arc<str>,
    pub id: usize,
    #[allow(dead_code)]
    pub node_type: NodeType,
    pub node_category: NodeCategory,
    pub context: HashMap<String, String>,
}
