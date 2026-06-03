use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_metrics::{
    NodeID, QueryID, QueryPlan, StatSnapshot, Stats,
    ops::{NodeCategory, NodeInfo, NodeType},
};

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
    TaskSubmit(TaskSubmitEvent),
    /// Driver-side: scheduler has decided which worker to dispatch the task
    /// to. Fires just before the handoff to the worker. The dashboard
    /// renders this as "running", but it precedes true execution start;
    /// see `TaskStart` (emitted by the worker) for the more precise signal.
    TaskScheduled(TaskScheduledEvent),
    TaskStart(TaskStartEvent),
    TaskEnd(TaskEndEvent),
    OperatorStart(OperatorStartEvent),
    OperatorEnd(OperatorEndEvent),
    Stats(StatsEvent),
    TaskStatsUpdate(TaskStatsUpdateEvent),
    ProcessStats(ProcessStatsEvent),
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
    pub origin_node_id: Option<NodeID>,
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
            origin_node_id: None,
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
pub struct ExecutionConfig {
    pub default_morsel_size: usize,
    pub scantask_max_parallel: usize,
    pub min_cpu_per_task: f64,

    pub enable_scan_task_split_and_merge: bool,
    pub scan_tasks_min_size_bytes: usize,
    pub scan_tasks_max_size_bytes: usize,
    pub max_sources_per_scan_task: usize,
    pub parquet_split_row_groups_max_files: usize,

    pub broadcast_join_size_bytes_threshold: usize,
    pub hash_join_partition_size_leniency: f64,

    pub shuffle_algorithm: String,
    pub shuffle_aggregation_default_partitions: usize,
    pub pre_shuffle_merge_threshold: usize,
    pub pre_shuffle_merge_partition_threshold: usize,

    pub partial_aggregation_threshold: usize,
    pub high_cardinality_aggregation_threshold: f64,

    pub maintain_order: bool,

    pub enable_dynamic_batching: bool,
    pub dynamic_batching_strategy: String,
}

impl From<&DaftExecutionConfig> for ExecutionConfig {
    fn from(config: &DaftExecutionConfig) -> Self {
        Self {
            default_morsel_size: config.default_morsel_size.get(),
            scantask_max_parallel: config.scantask_max_parallel,
            min_cpu_per_task: config.min_cpu_per_task,

            enable_scan_task_split_and_merge: config.enable_scan_task_split_and_merge,
            scan_tasks_min_size_bytes: config.scan_tasks_min_size_bytes,
            scan_tasks_max_size_bytes: config.scan_tasks_max_size_bytes,
            max_sources_per_scan_task: config.max_sources_per_scan_task,
            parquet_split_row_groups_max_files: config.parquet_split_row_groups_max_files,

            broadcast_join_size_bytes_threshold: config.broadcast_join_size_bytes_threshold,
            hash_join_partition_size_leniency: config.hash_join_partition_size_leniency,

            shuffle_algorithm: config.shuffle_algorithm.clone(),
            shuffle_aggregation_default_partitions: config.shuffle_aggregation_default_partitions,
            pre_shuffle_merge_threshold: config.pre_shuffle_merge_threshold,
            pre_shuffle_merge_partition_threshold: config.pre_shuffle_merge_partition_threshold,

            partial_aggregation_threshold: config.partial_aggregation_threshold,
            high_cardinality_aggregation_threshold: config.high_cardinality_aggregation_threshold,

            maintain_order: config.maintain_order,

            enable_dynamic_batching: config.enable_dynamic_batching,
            dynamic_batching_strategy: config.dynamic_batching_strategy.clone(),
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

/// Mid-execution per-task progress emitted by flotilla workers.
///
/// Batches all active tasks' scalar totals into a single event per worker per
/// tick so the dashboard can show in-flight progress separately from the
/// coordinator-aggregated operator stats. Per-operator breakdown is omitted
/// here on purpose — the local NodeID scope doesn't cleanly map to either the
/// distributed plan node id or a stable within-task position, and the only
/// current consumer is task-level scalars (cpu_us, rows, bytes). Bigger
/// breakdowns can be added as a separate event variant if a "drill into
/// running task" view ever needs them.
#[derive(Debug, Clone)]
pub struct TaskStatsUpdateEvent {
    pub header: EventHeader,
    pub tasks: Arc<Vec<TaskStatsSnapshot>>,
}

/// Per-task scalar totals reported mid-flight.
///
/// `cpu_us` is summed `DURATION_KEY` across the task's local pipeline
/// operators (busy time, not wall-clock since submit). The rows/bytes I/O
/// fields are filtered by `is_task_root`/`is_task_leaf` on each snapshot's
/// `NodeInfo` to avoid double-counting fused chains: only root snapshots
/// contribute external `rows.out`/`bytes.out`, only leaf snapshots contribute
/// external `rows.in`/`bytes.in`. See `daft_dashboard::engine::TaskTotals`.
#[derive(Debug, Clone, Default)]
pub struct TaskStatsSnapshot {
    pub task_id: u32,
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
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
    // pub execution_config: Option<ExecutionConfig>,
}

#[derive(Debug, Clone)]
pub struct ExecEndEvent {
    pub header: EventHeader,
    pub duration_ms: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub id: u32,
    /// The last distributed plan node in the task's pipeline — the one that
    /// dispatched the task. Matches `TaskContext::last_node_id`. Different
    /// from `LocalNodeContext::origin_node_id`, which attributes a single
    /// local plan node back to the distributed node that generated it.
    pub last_node_id: u32,
    pub node_ids: Vec<u32>,
    /// Fingerprint identifying tasks with functionally identical plans.
    /// Tasks with the same last_node_id + plan_fingerprint share a compiled pipeline.
    pub plan_fingerprint: u32,
    /// Optional human-readable task name (pipeline shape), set on submit.
    pub name: Option<Arc<str>>,
}

#[derive(Debug, Clone)]
pub struct TaskSubmitEvent {
    pub header: EventHeader,
    pub task: Arc<TaskInfo>,
    pub sources: Arc<Vec<TaskSource>>,
}

/// Driver-side "task is on its way to a worker". See `Event::TaskScheduled`.
#[derive(Debug, Clone)]
pub struct TaskScheduledEvent {
    pub header: EventHeader,
    pub task: Arc<TaskInfo>,
    pub worker_id: Option<Arc<str>>,
}

#[derive(Debug, Clone)]
pub struct TaskStartEvent {
    pub header: EventHeader,
    pub task: Arc<TaskInfo>,
    pub worker_id: Option<Arc<str>>,
}

#[derive(Debug, Clone)]
pub struct TaskEndEvent {
    pub header: EventHeader,
    pub task: Arc<TaskInfo>,
    pub worker_id: Option<Arc<str>>,
    pub outcome: TaskOutcome,
    pub stats: Vec<(Arc<NodeInfo>, StatSnapshot)>,
}

#[derive(Debug, Clone)]
pub enum TaskOutcome {
    Success,
    Failed { message: String },
    Cancelled,
}

#[derive(Debug, Clone)]
pub enum TaskSource {
    PhysicalScan(PhysicalScanSource),
    InMemoryScan(InMemoryScanSource),
}

#[derive(Debug, Clone)]
pub struct PhysicalScanSource {
    pub source_id: u32,
    pub scan_tasks: u32,
    pub paths: Vec<String>,
    pub storage_bytes: Option<usize>,
    pub estimated_memory_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct InMemoryScanSource {
    pub source_id: u32,
    pub partitions: usize,
    pub total_bytes: Option<usize>,
}
