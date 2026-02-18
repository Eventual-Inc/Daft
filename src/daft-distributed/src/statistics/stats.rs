use std::sync::{Arc, atomic::Ordering};

use common_metrics::{
    Counter, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot, TASK_ACTIVE_KEY, TASK_CANCELLED_KEY,
    TASK_COMPLETED_KEY, TASK_DURATION_KEY, TASK_FAILED_KEY, normalize_name, ops::NodeInfo,
    snapshot::DefaultSnapshot,
};
use opentelemetry::{
    KeyValue,
    metrics::{Meter, UpDownCounter},
};

use crate::{
    pipeline_node::{
        PipelineNodeContext,
        metrics::{key_values_from_context, key_values_from_node_info},
    },
    statistics::TaskEvent,
};

pub trait RuntimeStats: Send + Sync + 'static {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot);
    /// Returns the accumulated stats.
    fn export_snapshot(&self) -> StatSnapshot;
}
pub type RuntimeStatsRef = Arc<dyn RuntimeStats>;

pub struct RuntimeNodeManager {
    node_info: Arc<NodeInfo>,
    pub node_kv: Vec<KeyValue>,
    runtime_stats: RuntimeStatsRef,

    active_tasks: UpDownCounter<i64>,
    completed_tasks: Counter,
    failed_tasks: Counter,
    cancelled_tasks: Counter,
}

impl RuntimeNodeManager {
    pub fn new(meter: &Meter, runtime_stats: RuntimeStatsRef, node_info: Arc<NodeInfo>) -> Self {
        let node_kv = key_values_from_node_info(node_info.as_ref());
        Self {
            node_info,
            node_kv,
            runtime_stats,
            active_tasks: meter
                .i64_up_down_counter(normalize_name(TASK_ACTIVE_KEY))
                .build(),
            completed_tasks: Counter::new(meter, TASK_COMPLETED_KEY, None),
            failed_tasks: Counter::new(meter, TASK_FAILED_KEY, None),
            cancelled_tasks: Counter::new(meter, TASK_CANCELLED_KEY, None),
        }
    }

    /// Returns the accumulated stats for this node as (NodeInfo, StatSnapshot) for export to the driver.
    pub fn export_snapshot(&self) -> (Arc<NodeInfo>, StatSnapshot) {
        (self.node_info.clone(), self.runtime_stats.export_snapshot())
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
                    if node_info.id == self.node_info.id {
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

pub struct DefaultRuntimeStats {
    node_kv: Vec<KeyValue>,
    completed_rows_in: Counter,
    completed_rows_out: Counter,
    completed_cpu_us: Counter,
}

impl DefaultRuntimeStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);

        Self {
            node_kv,
            completed_rows_in: Counter::new(meter, ROWS_IN_KEY, None),
            completed_rows_out: Counter::new(meter, ROWS_OUT_KEY, None),
            completed_cpu_us: Counter::new(meter, TASK_DURATION_KEY, None),
        }
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        let StatSnapshot::Default(snapshot) = snapshot else {
            // TODO: Return immediately for now, but ideally should error
            return;
        };

        self.completed_cpu_us
            .add(snapshot.cpu_us, self.node_kv.as_slice());
        self.completed_rows_in
            .add(snapshot.rows_in, self.node_kv.as_slice());
        self.completed_rows_out
            .add(snapshot.rows_out, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.completed_cpu_us.load(Ordering::Relaxed),
            rows_in: self.completed_rows_in.load(Ordering::Relaxed),
            rows_out: self.completed_rows_out.load(Ordering::Relaxed),
        })
    }
}
