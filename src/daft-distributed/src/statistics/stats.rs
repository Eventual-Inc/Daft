use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

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
    /// Returns per-phase stats as (synthetic_node_id, snapshot) pairs.
    /// Default: no per-phase breakdown.
    fn export_phase_snapshots(&self) -> Vec<(usize, StatSnapshot)> {
        vec![]
    }
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

    /// Returns per-phase stats as (synthetic_node_id, Stats) pairs for dashboard emission.
    pub fn export_phase_snapshots(&self) -> Vec<(usize, StatSnapshot)> {
        self.runtime_stats.export_phase_snapshots()
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

                for (node_info, snapshot) in &stats.nodes {
                    // Local nodes are associated to this node through the node_origin_id
                    if self.node_info.node_origin_id == node_info.node_origin_id {
                        self.runtime_stats
                            .handle_worker_node_stats(node_info, snapshot);
                    }
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
    node_kv: Vec<KeyValue>,
}

impl BaseCounters {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
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

    pub fn export_default_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
        })
    }
}

/// Lightweight per-phase counters using simple atomics (no OpenTelemetry instrumentation).
pub struct SimpleCounters {
    duration_us: AtomicU64,
    rows_in: AtomicU64,
    rows_out: AtomicU64,
}

impl SimpleCounters {
    pub fn new() -> Self {
        Self {
            duration_us: AtomicU64::new(0),
            rows_in: AtomicU64::new(0),
            rows_out: AtomicU64::new(0),
        }
    }

    pub fn add_duration_us(&self, v: u64) {
        self.duration_us.fetch_add(v, Ordering::Relaxed);
    }

    pub fn add_rows_in(&self, v: u64) {
        self.rows_in.fetch_add(v, Ordering::Relaxed);
    }

    pub fn add_rows_out(&self, v: u64) {
        self.rows_out.fetch_add(v, Ordering::Relaxed);
    }

    pub fn export_default_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
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
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use std::sync::Arc;

    use common_metrics::{
        StatSnapshot,
        ops::NodeInfo,
        snapshot::DefaultSnapshot,
    };

    /// Create a NodeInfo with the given origin node id and optional phase.
    pub fn make_node_info(origin_node_id: usize, phase: Option<&str>) -> Arc<NodeInfo> {
        Arc::new(NodeInfo {
            name: "test".into(),
            id: origin_node_id,
            node_origin_id: origin_node_id,
            node_phase: phase.map(|s| s.to_string()),
            ..Default::default()
        })
    }

    /// Create a default StatSnapshot with the given rows and duration.
    pub fn make_default_snapshot(cpu_us: u64, rows_in: u64, rows_out: u64) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us,
            rows_in,
            rows_out,
        })
    }

    /// Extract (cpu_us, rows_in, rows_out) from a DefaultSnapshot.
    pub fn extract_default(snapshot: &StatSnapshot) -> (u64, u64, u64) {
        match snapshot {
            StatSnapshot::Default(s) => (s.cpu_us, s.rows_in, s.rows_out),
            _ => panic!("Expected DefaultSnapshot"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::test_utils::*;

    #[test]
    fn test_simple_counters_accumulate() {
        let c = SimpleCounters::new();
        c.add_duration_us(100);
        c.add_duration_us(200);
        c.add_rows_in(10);
        c.add_rows_out(5);

        let snap = c.export_default_snapshot();
        let (cpu, rows_in, rows_out) = extract_default(&snap);
        assert_eq!(cpu, 300);
        assert_eq!(rows_in, 10);
        assert_eq!(rows_out, 5);
    }

    #[test]
    fn test_simple_counters_empty() {
        let c = SimpleCounters::new();
        let snap = c.export_default_snapshot();
        let (cpu, rows_in, rows_out) = extract_default(&snap);
        assert_eq!(cpu, 0);
        assert_eq!(rows_in, 0);
        assert_eq!(rows_out, 0);
    }
}
