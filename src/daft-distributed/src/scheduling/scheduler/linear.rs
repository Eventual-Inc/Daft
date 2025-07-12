use std::collections::{BinaryHeap, HashMap};

use super::{PendingTask, ScheduledTask, Scheduler, WorkerSnapshot};
use crate::scheduling::{task::Task, worker::WorkerId};

#[allow(dead_code)]
pub(super) struct LinearScheduler<T: Task> {
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
    pending_tasks: BinaryHeap<PendingTask<T>>,
}

impl<T: Task> Default for LinearScheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl<T: Task> LinearScheduler<T> {
    pub fn new() -> Self {
        Self {
            worker_snapshots: HashMap::new(),
            pending_tasks: BinaryHeap::new(),
        }
    }
}

impl<T: Task> Scheduler<T> for LinearScheduler<T> {
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

    fn enqueue_tasks(&mut self, _tasks: Vec<PendingTask<T>>) {
        todo!("FLOTILLA_MS1: Implement enqueue_tasks for linear scheduler")
    }

    fn schedule_tasks(&mut self) -> Vec<ScheduledTask<T>> {
        todo!("FLOTILLA_MS1: Implement get_schedulable_tasks for linear scheduler")
    }

    fn num_pending_tasks(&self) -> usize {
        self.pending_tasks.len()
    }

    fn get_autoscaling_request(&mut self) -> Option<usize> {
        todo!("FLOTILLA_MS1: Implement get_autoscaling_request for linear scheduler")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test_linear_scheduler_spread_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler spread scheduling")
    }

    #[test]
    #[ignore]
    fn test_linear_scheduler_node_affinity_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler node affinity scheduling")
    }

    #[test]
    #[ignore]
    fn test_linear_scheduler_hard_node_affinity_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler hard node affinity scheduling")
    }

    #[test]
    #[ignore]
    fn test_linear_scheduler_soft_node_affinity_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler soft node affinity scheduling")
    }

    #[test]
    #[ignore]
    fn test_linear_scheduler_with_mixed_scheduling_strategies() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler with mixed scheduling strategies")
    }

    #[test]
    #[ignore]
    fn test_linear_scheduler_with_priority_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler with priority scheduling")
    }

    #[test]
    #[ignore]
    fn test_linear_scheduler_with_no_workers() {
        todo!("FLOTILLA_MS1: Implement test for linear scheduler with no workers")
    }
}
