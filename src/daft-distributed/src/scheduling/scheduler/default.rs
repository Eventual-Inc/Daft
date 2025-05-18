use std::collections::{BinaryHeap, HashMap};

use super::{scheduler_actor::SchedulableTask, ScheduledTask, Scheduler, WorkerSnapshot};
use crate::scheduling::{
    task::{SchedulingStrategy, Task},
    worker::WorkerId,
};

#[allow(dead_code)]
pub(super) struct DefaultScheduler<T: Task> {
    pending_tasks: BinaryHeap<SchedulableTask<T>>,
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
}

#[allow(dead_code)]
impl<T: Task> DefaultScheduler<T> {
    pub fn new() -> Self {
        Self {
            pending_tasks: BinaryHeap::new(),
            worker_snapshots: HashMap::new(),
        }
    }
    fn try_schedule_spread_task(&self) -> Option<WorkerId> {
        todo!("FLOTILLA_MS1: Implement scheduling spread task for default scheduler")
    }

    fn try_schedule_soft_node_affinity_task(&self, _node_id: &str) -> Option<WorkerId> {
        todo!("FLOTILLA_MS1: Implement scheduling soft node affinity task for default scheduler")
    }

    fn try_schedule_hard_node_affinity_task(&self, _node_id: &str) -> Option<WorkerId> {
        todo!("FLOTILLA_MS1: Implement scheduling hard node affinity task for default scheduler")
    }

    fn try_schedule_task(&self, task: &SchedulableTask<T>) -> Option<WorkerId> {
        match task.strategy() {
            SchedulingStrategy::Spread => self.try_schedule_spread_task(),
            SchedulingStrategy::NodeAffinity { node_id, soft } => match soft {
                true => self.try_schedule_soft_node_affinity_task(node_id),
                false => self.try_schedule_hard_node_affinity_task(node_id),
            },
        }
    }
}

impl<T: Task> Scheduler<T> for DefaultScheduler<T> {
    fn enqueue_tasks(&mut self, tasks: Vec<SchedulableTask<T>>) {
        self.pending_tasks.extend(tasks);
    }

    fn get_schedulable_tasks(&mut self) -> Vec<ScheduledTask<T>> {
        let mut scheduled = Vec::new();
        while let Some(task_ref) = self.pending_tasks.peek() {
            if let Some(worker_id) = self.try_schedule_task(task_ref) {
                let task = self
                    .pending_tasks
                    .pop()
                    .expect("Task should be present in pending tasks");
                scheduled.push(ScheduledTask { task, worker_id });
            }
        }
        scheduled
    }

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_scheduler_spread_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for default scheduler spread scheduling")
    }

    #[test]
    fn test_default_scheduler_soft_node_affinity_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for default scheduler node affinity scheduling")
    }

    #[test]
    fn test_default_scheduler_hard_node_affinity_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for default scheduler node affinity scheduling")
    }

    #[test]
    fn test_default_scheduler_with_mixed_scheduling_strategies() {
        todo!("FLOTILLA_MS1: Implement test for default scheduler with mixed scheduling strategies")
    }

    #[test]
    fn test_default_scheduler_with_priority_scheduling() {
        todo!("FLOTILLA_MS1: Implement test for default scheduler with priority scheduling")
    }

    #[test]
    fn test_default_scheduler_with_no_workers() {
        todo!("FLOTILLA_MS1: Implement test for default scheduler with no workers")
    }
}
