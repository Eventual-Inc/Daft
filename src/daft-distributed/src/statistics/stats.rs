use std::sync::{Arc, atomic::Ordering};

use common_metrics::{
    Counter, Meter, StatSnapshot, TASK_ACTIVE_KEY, TASK_CANCELLED_KEY, TASK_COMPLETED_KEY,
    TASK_FAILED_KEY, UNIT_TASKS, UpDownCounter,
    ops::NodeInfo,
    snapshot::{DefaultSnapshot, StatSnapshotImpl as _},
};
use opentelemetry::KeyValue;

use crate::{
    pipeline_node::{PipelineNodeContext, metrics::key_values_from_context},
    statistics::TaskEvent,
};

pub trait RuntimeStats: Send + Sync + 'static {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot);
    /// Returns the accumulated stats.
    fn export_snapshot(&self) -> StatSnapshot;
    /// Increment the number of tasks originated from this distributed node's `origin_node_id`.
    fn add_num_tasks(&self, num_tasks: u64);
}
pub type RuntimeStatsRef = Arc<dyn RuntimeStats>;

pub struct RuntimeNodeManager {
    node_info: Arc<NodeInfo>,
    pub node_kv: Vec<KeyValue>,
    runtime_stats: RuntimeStatsRef,

    active_tasks: UpDownCounter,
    completed_tasks: Counter,
    failed_tasks: Counter,
    cancelled_tasks: Counter,
}

impl RuntimeNodeManager {
    pub fn new(meter: &Meter, runtime_stats: RuntimeStatsRef, node_info: Arc<NodeInfo>) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            node_info,
            node_kv,
            runtime_stats,
            active_tasks: meter.i64_up_down_counter(TASK_ACTIVE_KEY),
            completed_tasks: meter.u64_counter_with_desc_and_unit(
                TASK_COMPLETED_KEY,
                None,
                Some(UNIT_TASKS.into()),
            ),
            failed_tasks: meter.u64_counter_with_desc_and_unit(
                TASK_FAILED_KEY,
                None,
                Some(UNIT_TASKS.into()),
            ),
            cancelled_tasks: meter.u64_counter_with_desc_and_unit(
                TASK_CANCELLED_KEY,
                None,
                Some(UNIT_TASKS.into()),
            ),
        }
    }

    /// Returns the accumulated stats for this node as (NodeInfo, StatSnapshot) for export to the driver.
    pub fn export_snapshot(&self) -> (Arc<NodeInfo>, StatSnapshot) {
        (self.node_info.clone(), self.runtime_stats.export_snapshot())
    }

    /// Number of tasks currently in flight for this node. Clamped at 0 —
    /// increments and decrements are paired through `handle_task_event`,
    /// so the underlying signed counter should not observably go negative.
    pub fn active_task_count(&self) -> u64 {
        self.active_tasks.load(Ordering::Relaxed).max(0) as u64
    }

    pub fn completed_task_count(&self) -> u64 {
        self.completed_tasks.load(Ordering::Relaxed)
    }

    pub fn failed_task_count(&self) -> u64 {
        self.failed_tasks.load(Ordering::Relaxed)
    }

    pub fn cancelled_task_count(&self) -> u64 {
        self.cancelled_tasks.load(Ordering::Relaxed)
    }

    fn dec_active_tasks(&self) {
        self.active_tasks.add(-1, self.node_kv.as_slice());
    }

    pub fn handle_task_event(&self, event: &TaskEvent) {
        match event {
            TaskEvent::Scheduled { .. } => {
                self.active_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::Completed { stats, .. } => {
                self.dec_active_tasks();
                self.completed_tasks.add(1, self.node_kv.as_slice());

                let mut originated_here = false;
                for (node_info, snapshot) in &stats.nodes {
                    // Local nodes are associated to this node through the node_origin_id
                    if self.node_info.node_origin_id == node_info.node_origin_id {
                        originated_here = true;
                        self.runtime_stats
                            .handle_worker_node_stats(node_info, snapshot);
                    }
                }
                if originated_here {
                    self.runtime_stats.add_num_tasks(1);
                }
            }
            TaskEvent::Failed { .. } => {
                self.dec_active_tasks();
                self.failed_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::Cancelled { .. } => {
                self.dec_active_tasks();
                self.cancelled_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::Submitted { .. } => (), // We don't track submitted tasks
        }
    }
}

pub struct BaseCounters {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    num_tasks: Counter,
    node_kv: Vec<KeyValue>,
}

impl BaseCounters {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_in: meter.bytes_in_metric(),
            bytes_out: meter.bytes_out_metric(),
            num_tasks: meter.num_tasks_metric(),
            node_kv,
        }
    }

    pub fn add_duration_us(&self, v: u64) {
        self.duration_us.add(v, self.node_kv.as_slice());
    }

    pub fn add_rows_in(&self, v: u64) {
        self.rows_in.add(v, self.node_kv.as_slice());
    }

    pub fn add_rows_out(&self, v: u64) {
        self.rows_out.add(v, self.node_kv.as_slice());
    }

    pub fn add_bytes_in(&self, v: u64) {
        self.bytes_in.add(v, self.node_kv.as_slice());
    }

    pub fn add_bytes_out(&self, v: u64) {
        self.bytes_out.add(v, self.node_kv.as_slice());
    }

    pub fn add_num_tasks(&self, v: u64) {
        self.num_tasks.add(v, self.node_kv.as_slice());
    }

    pub fn export_default_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            num_tasks: self.num_tasks.load(Ordering::Relaxed),
        })
    }
}

pub struct DefaultRuntimeStats {
    base: BaseCounters,
}

impl DefaultRuntimeStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        Self {
            base: BaseCounters::new(meter, context),
        }
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        self.base.add_duration_us(snapshot.duration_us());

        let StatSnapshot::Default(snapshot) = snapshot else {
            // TODO: Return immediately for now, but ideally should error
            return;
        };

        self.base.add_rows_in(snapshot.rows_in);
        self.base.add_rows_out(snapshot.rows_out);
        self.base.add_bytes_in(snapshot.bytes_in);
        self.base.add_bytes_out(snapshot.bytes_out);
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }

    fn add_num_tasks(&self, num_tasks: u64) {
        self.base.add_num_tasks(num_tasks);
    }
}

#[cfg(test)]
mod tests {
    use common_metrics::{Meter, ops::NodeInfo};
    use daft_local_plan::ExecutionStats;

    use super::*;
    use crate::{
        pipeline_node::PipelineNodeContext, scheduling::task::TaskContext, statistics::TaskEvent,
    };

    fn context() -> PipelineNodeContext {
        PipelineNodeContext::new(
            0,
            "test-query".into(),
            7,
            "Mock".into(),
            common_metrics::ops::NodeType::Project,
            common_metrics::ops::NodeCategory::Intermediate,
        )
    }

    fn default_num_tasks(snapshot: &StatSnapshot) -> u64 {
        match snapshot {
            StatSnapshot::Default(s) => s.num_tasks,
            other => panic!("expected Default snapshot, got {other:?}"),
        }
    }

    #[test]
    fn base_counters_num_tasks_round_trips_via_snapshot() {
        let meter = Meter::test_scope("base_counters_num_tasks");
        let base = BaseCounters::new(&meter, &context());
        base.add_num_tasks(3);
        base.add_num_tasks(4);
        assert_eq!(default_num_tasks(&base.export_default_snapshot()), 7);
    }

    fn runtime_node_manager(node_origin_id: usize) -> RuntimeNodeManager {
        let meter = Meter::test_scope("runtime_node_manager_num_tasks");
        let node_info = Arc::new(NodeInfo {
            id: node_origin_id,
            node_origin_id,
            ..Default::default()
        });
        let runtime_stats = Arc::new(DefaultRuntimeStats::new(&meter, &context()));
        RuntimeNodeManager::new(&meter, runtime_stats, node_info)
    }

    fn completed_event(worker_node_origin_ids: &[usize]) -> TaskEvent {
        let nodes = worker_node_origin_ids
            .iter()
            .map(|&origin| {
                (
                    Arc::new(NodeInfo {
                        node_origin_id: origin,
                        ..Default::default()
                    }),
                    StatSnapshot::Default(DefaultSnapshot {
                        cpu_us: 0,
                        rows_in: 0,
                        rows_out: 0,
                        bytes_in: 0,
                        bytes_out: 0,
                        num_tasks: 0,
                    }),
                )
            })
            .collect();
        TaskEvent::Completed {
            context: TaskContext::default(),
            stats: ExecutionStats::new("q".into(), nodes),
        }
    }

    #[test]
    fn handle_task_event_increments_num_tasks_on_origin_match() {
        let mgr = runtime_node_manager(42);
        mgr.handle_task_event(&completed_event(&[42]));
        mgr.handle_task_event(&completed_event(&[42, 42]));
        // Two tasks matched; each increments by 1 regardless of how many
        // worker-node snapshots inside share the same origin_node_id.
        let (_, snapshot) = mgr.export_snapshot();
        assert_eq!(default_num_tasks(&snapshot), 2);
    }

    #[test]
    fn handle_task_event_does_not_increment_num_tasks_without_origin_match() {
        let mgr = runtime_node_manager(42);
        mgr.handle_task_event(&completed_event(&[1, 2, 3]));
        let (_, snapshot) = mgr.export_snapshot();
        assert_eq!(default_num_tasks(&snapshot), 0);
    }

    fn scheduled_event() -> TaskEvent {
        TaskEvent::Scheduled {
            context: TaskContext::default(),
        }
    }

    fn failed_event() -> TaskEvent {
        TaskEvent::Failed {
            context: TaskContext::default(),
            reason: "boom".into(),
        }
    }

    fn cancelled_event() -> TaskEvent {
        TaskEvent::Cancelled {
            context: TaskContext::default(),
        }
    }

    #[test]
    fn task_count_getters_reflect_task_events() {
        let mgr = runtime_node_manager(42);
        // 3 scheduled; 1 completes, 1 fails, 1 is cancelled — net active 0.
        mgr.handle_task_event(&scheduled_event());
        mgr.handle_task_event(&scheduled_event());
        mgr.handle_task_event(&scheduled_event());
        mgr.handle_task_event(&completed_event(&[42]));
        mgr.handle_task_event(&failed_event());
        mgr.handle_task_event(&cancelled_event());

        assert_eq!(mgr.active_task_count(), 0);
        assert_eq!(mgr.completed_task_count(), 1);
        assert_eq!(mgr.failed_task_count(), 1);
        assert_eq!(mgr.cancelled_task_count(), 1);
    }

    #[test]
    fn active_task_count_clamps_at_zero_if_never_scheduled() {
        // Failure without a prior Scheduled would push the underlying
        // signed counter below zero; the getter must not leak a negative.
        let mgr = runtime_node_manager(42);
        mgr.handle_task_event(&failed_event());
        assert_eq!(mgr.active_task_count(), 0);
        assert_eq!(mgr.failed_task_count(), 1);
    }
}
