use std::sync::atomic::{AtomicU64, Ordering};

use common_error::DaftResult;
use opentelemetry::{
    global,
    metrics::{Counter, Gauge},
};

use crate::{pipeline_node::NodeID, statistics::TaskEvent};

pub trait RuntimeStats: Send + Sync + 'static {
    fn handle_task_event(&self, event: &TaskEvent) -> DaftResult<()>;
}

pub struct DefaultRuntimeStats {
    active_tasks_count: AtomicU64,
    active_tasks_gauge: Gauge<u64>,
    completed_tasks: Counter<u64>,
    failed_tasks: Counter<u64>,
    cancelled_tasks: Counter<u64>,
}

impl DefaultRuntimeStats {
    pub fn new(node_id: NodeID) -> Self {
        let meter = global::meter("FlotillaScheduler");

        Self {
            active_tasks_count: AtomicU64::new(0),
            active_tasks_gauge: meter.u64_gauge(format!("{}.active_tasks", node_id)).build(),
            completed_tasks: meter
                .u64_counter(format!("{}.completed_tasks", node_id))
                .build(),
            failed_tasks: meter
                .u64_counter(format!("{}.failed_tasks", node_id))
                .build(),
            cancelled_tasks: meter
                .u64_counter(format!("{}.cancelled_tasks", node_id))
                .build(),
        }
    }

    fn inc_active_tasks(&self) {
        self.active_tasks_count.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "Incrementing active tasks count to: {:?}",
            self.active_tasks_count.load(Ordering::Relaxed)
        );
        self.active_tasks_gauge
            .record(self.active_tasks_count.load(Ordering::Relaxed), &[]);
    }

    fn dec_active_tasks(&self) {
        self.active_tasks_count.fetch_sub(1, Ordering::Relaxed);
        eprintln!(
            "Decrementing active tasks count to: {:?}",
            self.active_tasks_count.load(Ordering::Relaxed)
        );
        self.active_tasks_gauge
            .record(self.active_tasks_count.load(Ordering::Relaxed), &[]);
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
                eprintln!("Incrementing completed tasks count");
                self.completed_tasks.add(1, &[]);
            }
            TaskEvent::TaskFailed { .. } => {
                self.dec_active_tasks();
                eprintln!("Incrementing failed tasks count");
                self.failed_tasks.add(1, &[]);
            }
            TaskEvent::TaskCancelled { .. } => {
                self.dec_active_tasks();
                eprintln!("Incrementing cancelled tasks count");
                self.cancelled_tasks.add(1, &[]);
            }
            TaskEvent::TaskSubmitted { .. } => (), // We don't track submitted tasks
        }

        Ok(())
    }
}
