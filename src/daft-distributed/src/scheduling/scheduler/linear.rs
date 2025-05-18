use std::collections::HashMap;

use super::{scheduler_actor::SchedulableTask, ScheduledTask, Scheduler, WorkerSnapshot};
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

    fn get_schedulable_tasks(&mut self) -> Vec<ScheduledTask<T>> {
        todo!("FLOTILLA_MS1: Implement get_schedulable_tasks for linear scheduler")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_scheduler_spread_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler spread scheduling")
    }

    #[test]
    fn test_linear_scheduler_node_affinity_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler node affinity scheduling")
    }

    #[test]
    fn test_linear_scheduler_hard_node_affinity_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler hard node affinity scheduling")
    }

    #[test]
    fn test_linear_scheduler_soft_node_affinity_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler soft node affinity scheduling")
    }

    #[test]
    fn test_linear_scheduler_with_mixed_scheduling_strategies() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler with mixed scheduling strategies")
    }

    #[test]
    fn test_linear_scheduler_with_priority_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler with priority scheduling")
    }

    #[test]
    fn test_linear_scheduler_with_no_workers() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler with no workers")
    }
}
