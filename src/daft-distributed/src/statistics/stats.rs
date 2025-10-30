use common_error::DaftResult;
use opentelemetry::{
    global,
    metrics::{Counter, Meter, UpDownCounter},
};

use crate::{pipeline_node::NodeID, statistics::TaskEvent};

pub trait RuntimeStats: Send + Sync + 'static {
    fn handle_task_event(&self, event: &TaskEvent) -> DaftResult<()>;
}

#[allow(clippy::struct_field_names)]
pub struct DefaultRuntimeStats {
    active_tasks: UpDownCounter<i64>,
    completed_tasks: Counter<u64>,
    failed_tasks: Counter<u64>,
    cancelled_tasks: Counter<u64>,
}

impl DefaultRuntimeStats {
    pub fn new_impl(meter: &Meter, node_id: NodeID) -> Self {
        Self {
            active_tasks: meter
                .i64_up_down_counter(format!("{}.active_tasks", node_id))
                .build(),
            completed_tasks: meter
                .u64_counter(format!("daft.{}.completed_tasks", node_id))
                .build(),
            failed_tasks: meter
                .u64_counter(format!("daft.{}.failed_tasks", node_id))
                .build(),
            cancelled_tasks: meter
                .u64_counter(format!("daft.{}.cancelled_tasks", node_id))
                .build(),
        }
    }

    pub fn new(node_id: NodeID) -> Self {
        Self::new_impl(&global::meter("DistributedNodeStats-Default"), node_id)
    }

    fn inc_active_tasks(&self) {
        self.active_tasks.add(1, &[]);
    }

    fn dec_active_tasks(&self) {
        self.active_tasks.add(-1, &[]);
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
                self.completed_tasks.add(1, &[]);
            }
            TaskEvent::TaskFailed { .. } => {
                self.dec_active_tasks();
                self.failed_tasks.add(1, &[]);
            }
            TaskEvent::TaskCancelled { .. } => {
                self.dec_active_tasks();
                self.cancelled_tasks.add(1, &[]);
            }
            TaskEvent::TaskSubmitted { .. } => (), // We don't track submitted tasks
        }

        Ok(())
    }
}
