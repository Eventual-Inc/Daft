use common_error::DaftResult;
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Meter, UpDownCounter},
};

use crate::{pipeline_node::NodeID, statistics::TaskEvent};

pub trait RuntimeStats: Send + Sync + 'static {
    fn handle_task_event(&self, event: &TaskEvent) -> DaftResult<()>;
}

#[allow(clippy::struct_field_names)]
pub struct DefaultRuntimeStats {
    pub node_kv: Vec<KeyValue>,
    active_tasks: UpDownCounter<i64>,
    completed_tasks: Counter<u64>,
    failed_tasks: Counter<u64>,
    cancelled_tasks: Counter<u64>,
}

impl DefaultRuntimeStats {
    pub fn new_impl(meter: &Meter, node_id: NodeID) -> Self {
        let node_kv = KeyValue::new("node_id", node_id.to_string());
        Self {
            node_kv: vec![node_kv],
            active_tasks: meter
                .i64_up_down_counter("daft.distributed.node_stats.active_tasks")
                .build(),
            completed_tasks: meter
                .u64_counter("daft.distributed.node_stats.completed_tasks")
                .build(),
            failed_tasks: meter
                .u64_counter("daft.distributed.node_stats.failed_tasks")
                .build(),
            cancelled_tasks: meter
                .u64_counter("daft.distributed.node_stats.cancelled_tasks")
                .build(),
        }
    }

    pub fn new(node_id: NodeID) -> Self {
        Self::new_impl(&global::meter("daft.distributed.node_stats"), node_id)
    }

    fn inc_active_tasks(&self) {
        self.active_tasks.add(1, self.node_kv.as_slice());
    }

    fn dec_active_tasks(&self) {
        self.active_tasks.add(-1, self.node_kv.as_slice());
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn handle_task_event(&self, event: &TaskEvent) -> DaftResult<()> {
        match event {
            TaskEvent::TaskScheduled { .. } => {
                self.inc_active_tasks();
            }
            TaskEvent::TaskCompleted { .. } => {
                self.dec_active_tasks();
                self.completed_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::TaskFailed { .. } => {
                self.dec_active_tasks();
                self.failed_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::TaskCancelled { .. } => {
                self.dec_active_tasks();
                self.cancelled_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::TaskSubmitted { .. } => (), // We don't track submitted tasks
        }

        Ok(())
    }
}
