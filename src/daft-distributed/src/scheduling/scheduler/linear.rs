use std::collections::HashMap;

use super::{scheduler_actor::SchedulableTask, Scheduler, WorkerSnapshot};
use crate::scheduling::{task::Task, worker::WorkerId};

#[allow(dead_code)]
pub(crate) struct LinearScheduler {
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
}

#[allow(dead_code)]
impl LinearScheduler {
    pub fn new() -> Self {
        Self {
            worker_snapshots: HashMap::new(),
        }
    }
}

impl<T: Task> Scheduler<T> for LinearScheduler {
    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]) {
        for worker_snapshot in worker_snapshots {
            if let Some(existing_snapshot) =
                self.worker_snapshots.get_mut(&worker_snapshot.worker_id)
            {
                *existing_snapshot = worker_snapshot.clone();
            } else {
                self.worker_snapshots
                    .insert(worker_snapshot.worker_id.clone(), worker_snapshot.clone());
            }
        }
    }

    fn enqueue_tasks(&mut self, _tasks: Vec<SchedulableTask<T>>) {
        todo!("FLOTILLA_MS1: Implement enqueue_tasks for linear scheduler")
    }

    fn get_scheduled_tasks(&mut self) -> Vec<(WorkerId, SchedulableTask<T>)> {
        todo!("FLOTILLA_MS1: Implement get_scheduled_tasks for linear scheduler")
    }
}
