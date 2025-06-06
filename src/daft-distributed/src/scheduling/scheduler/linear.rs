use std::collections::{BinaryHeap, HashMap};

use super::{SchedulableTask, ScheduledTask, Scheduler, WorkerSnapshot};
use crate::scheduling::{
    task::{SchedulingStrategy, Task, TaskDetails},
    worker::WorkerId,
};

#[allow(dead_code)]
pub(super) struct LinearScheduler<T: Task> {
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
    pending_tasks: BinaryHeap<SchedulableTask<T>>,
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

    // Spread scheduling: Schedule tasks to the worker with the most available slots, to
    // TODO: Change the approach to instead spread based on tasks of the same 'type', i.e. from the same pipeline node.
    fn try_schedule_spread_task(&self, task: &T) -> Option<WorkerId> {
        self.worker_snapshots
            .iter()
            .filter(|(_, worker)| worker.can_schedule_task(task))
            .max_by_key(|(_, worker)| {
                (worker.available_num_cpus() + worker.available_num_gpus()) as usize
            })
            .map(|(id, _)| id.clone())
    }

    // Soft worker affinity scheduling: Schedule task to the worker if it has capacity
    // Otherwise, fallback to spread scheduling
    fn try_schedule_worker_affinity_task(
        &self,
        task: &T,
        worker_id: &WorkerId,
        soft: bool,
    ) -> Option<WorkerId> {
        if let Some(worker) = self.worker_snapshots.get(worker_id) {
            if worker.can_schedule_task(task) {
                return Some(worker.worker_id.clone());
            }
        }
        // Fallback to spread scheduling if soft is true
        if soft {
            self.try_schedule_spread_task(task)
        } else {
            None
        }
    }

    fn try_schedule_task(&self, task: &SchedulableTask<T>) -> Option<WorkerId> {
        match task.strategy() {
            SchedulingStrategy::Spread => self.try_schedule_spread_task(&task.task),
            SchedulingStrategy::WorkerAffinity { worker_id, soft } => {
                self.try_schedule_worker_affinity_task(&task.task, worker_id, *soft)
            }
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

    fn enqueue_tasks(&mut self, tasks: Vec<SchedulableTask<T>>) {
        self.pending_tasks.extend(tasks);
    }

    fn get_schedulable_tasks(&mut self) -> Vec<ScheduledTask<T>> {
        // Check if any worker has active tasks
        let has_active_tasks = self
            .worker_snapshots
            .values()
            .any(|worker| !worker.active_task_details.is_empty());

        // If there are active tasks, don't schedule any new ones
        if has_active_tasks {
            return Vec::new();
        }

        let mut scheduled = Vec::new();
        let mut unscheduled = Vec::new();
        while let Some(task) = self.pending_tasks.pop() {
            if let Some(worker_id) = self.try_schedule_task(&task) {
                self.worker_snapshots
                    .get_mut(&worker_id)
                    .expect("Worker should be present in LinearScheduler")
                    .active_task_details
                    .insert(task.task_id().clone(), TaskDetails::from(&task.task));
                scheduled.push(ScheduledTask { task, worker_id });
                break;
            } else {
                unscheduled.push(task);
            }
        }
        self.pending_tasks.extend(unscheduled);
        scheduled
    }

    fn num_pending_tasks(&self) -> usize {
        self.pending_tasks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
