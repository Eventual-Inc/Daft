use std::sync::{Arc, atomic::Ordering};

use common_metrics::{
    Counter, DURATION_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot, TASK_ACTIVE_KEY,
    TASK_CANCELLED_KEY, TASK_COMPLETED_KEY, TASK_FAILED_KEY, UNIT_MICROSECONDS, UNIT_ROWS,
    UNIT_TASKS, normalize_name, ops::NodeInfo, snapshot::DefaultSnapshot,
};
use opentelemetry::{
    KeyValue,
    metrics::{Meter, UpDownCounter},
};

use crate::{
    pipeline_node::{PipelineNodeContext, metrics::key_values_from_context},
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
        let node_kv = node_info.to_key_values();
        Self {
            node_info,
            node_kv,
            runtime_stats,
            active_tasks: meter
                .i64_up_down_counter(normalize_name(TASK_ACTIVE_KEY))
                .build(),
            completed_tasks: Counter::new(meter, TASK_COMPLETED_KEY, None, Some(UNIT_TASKS.into())),
            failed_tasks: Counter::new(meter, TASK_FAILED_KEY, None, Some(UNIT_TASKS.into())),
            cancelled_tasks: Counter::new(meter, TASK_CANCELLED_KEY, None, Some(UNIT_TASKS.into())),
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
            duration_us: Counter::new(meter, DURATION_KEY, None, Some(UNIT_MICROSECONDS.into())),
            rows_in: Counter::new(meter, ROWS_IN_KEY, None, Some(UNIT_ROWS.into())),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None, Some(UNIT_ROWS.into())),
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
        let StatSnapshot::Default(snapshot) = snapshot else {
            // TODO: Return immediately for now, but ideally should error
            return;
        };

        self.base.add_duration_us(snapshot.cpu_us);
        self.base.add_rows_in(snapshot.rows_in);
        self.base.add_rows_out(snapshot.rows_out);
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }
}
