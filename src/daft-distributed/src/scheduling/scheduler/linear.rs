use std::collections::{BinaryHeap, HashMap};

use super::{PendingTask, ScheduledTask, Scheduler, WorkerSnapshot};
use crate::scheduling::{
    task::{SchedulingStrategy, Task, TaskDetails, TaskResourceRequest},
    worker::WorkerId,
};

pub(super) struct LinearScheduler<T: Task> {
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
    pending_tasks: BinaryHeap<PendingTask<T>>,
}

impl<T: Task> Default for LinearScheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Task> LinearScheduler<T> {
    pub fn new() -> Self {
        Self {
            worker_snapshots: HashMap::new(),
            pending_tasks: BinaryHeap::new(),
        }
    }

    fn try_schedule_spread_task(&self, task: &T) -> Option<WorkerId> {
        self.worker_snapshots
            .iter()
            .filter(|(_, worker)| worker.can_schedule_task(task))
            .max_by_key(|(_, worker)| {
                (worker.available_num_cpus() + worker.available_num_gpus()) as usize
            })
            .map(|(id, _)| id.clone())
    }

    fn try_schedule_worker_affinity_task(
        &self,
        task: &T,
        worker_id: &WorkerId,
        soft: bool,
    ) -> Option<WorkerId> {
        if let Some(worker) = self.worker_snapshots.get(worker_id)
            && worker.can_schedule_task(task)
        {
            return Some(worker.worker_id.clone());
        }
        // Fallback to spread scheduling if soft is true
        if soft {
            self.try_schedule_spread_task(task)
        } else {
            None
        }
    }

    fn try_schedule_task(&self, task: &PendingTask<T>) -> Option<WorkerId> {
        match task.strategy() {
            SchedulingStrategy::Spread => self.try_schedule_spread_task(&task.task),
            SchedulingStrategy::WorkerAffinity { worker_id, soft } => {
                self.try_schedule_worker_affinity_task(&task.task, worker_id, *soft)
            }
        }
    }

    fn needs_autoscaling(&self) -> bool {
        // If there are no pending tasks, we don't need to autoscale
        if self.pending_tasks.is_empty() {
            return false;
        }

        // If there are no workers, we need to autoscale
        if self.worker_snapshots.is_empty() {
            return true;
        }

        false
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

    fn enqueue_tasks(&mut self, tasks: Vec<PendingTask<T>>) {
        self.pending_tasks.extend(tasks);
    }

    fn schedule_tasks(&mut self) -> Vec<ScheduledTask<T>> {
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

        // Process all tasks in the queue
        while let Some(task) = self.pending_tasks.pop() {
            if let Some(worker_id) = self.try_schedule_task(&task) {
                self.worker_snapshots
                    .get_mut(&worker_id)
                    .unwrap()
                    .active_task_details
                    .insert(task.task_context(), TaskDetails::from(&task.task));
                scheduled.push(ScheduledTask::new(task, worker_id));
                // Only schedule one task
                break;
            } else {
                unscheduled.push(task);
            }
        }

        // Put unscheduled tasks back in the queue
        self.pending_tasks.extend(unscheduled);
        scheduled
    }

    fn num_pending_tasks(&self) -> usize {
        self.pending_tasks.len()
    }

    fn get_autoscaling_request(&mut self) -> Option<Vec<TaskResourceRequest>> {
        // If we need to autoscale, return the resource requests of the pending tasks
        let needs_autoscaling = self.needs_autoscaling();
        needs_autoscaling.then(|| {
            self.pending_tasks
                .iter()
                .next()
                .map(|task| vec![task.task.resource_request().clone()])
                .unwrap_or_default()
        })
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::scheduling::{
        scheduler::test_utils::{
            create_schedulable_task, create_spread_task, create_worker_affinity_task,
            setup_scheduler, setup_workers,
        },
        task::tests::{MockTask, MockTaskBuilder},
    };

    #[test]
    fn test_linear_scheduler_spread_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1),
            (worker_2.clone(), 2),
            (worker_3.clone(), 3),
        ]);

        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Spread strategy
        let tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
        ];

        // Enqueue and schedule tasks
        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        // Only one task should be scheduled because of linear scheduling
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Try to schedule more tasks - should fail because one task is already running
        let result = scheduler.schedule_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Update worker state to reflect that the workers are all idle
        let worker_snapshots = workers
            .values()
            .map(|worker| WorkerSnapshot::from(worker))
            .collect::<Vec<_>>();
        scheduler.update_worker_state(&worker_snapshots);

        // Now we should be able to schedule another task
        let result = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);
    }

    #[test]
    fn test_linear_scheduler_soft_node_affinity_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1),
            (worker_2.clone(), 1),
            (worker_3.clone(), 2),
        ]);

        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with soft Node Affinity strategies
        let tasks = vec![
            create_worker_affinity_task(&worker_1, true, Some(1)),
            create_worker_affinity_task(&worker_2, true, Some(2)),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        // Only one task should be scheduled
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        // Verify the scheduled task went to its preferred worker
        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }

        let result = scheduler.schedule_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        // Update worker state to reflect that the workers are all idle
        let worker_snapshots = workers
            .values()
            .map(|worker| WorkerSnapshot::from(worker))
            .collect::<Vec<_>>();
        scheduler.update_worker_state(&worker_snapshots);

        let result = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 0);

        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }
    }

    #[test]
    fn test_linear_scheduler_hard_node_affinity_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1),
            (worker_2.clone(), 2),
            (worker_3.clone(), 3),
        ]);

        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with hard Node Affinity strategies
        let tasks = vec![
            create_worker_affinity_task(&worker_1, false, Some(1)),
            create_worker_affinity_task(&worker_2, false, Some(2)),
            create_worker_affinity_task(&worker_3, false, Some(3)),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        // Only one task should be scheduled
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Verify the scheduled task went to its preferred worker
        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }

        let result = scheduler.schedule_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Update worker state to reflect that the workers are all idle
        let worker_snapshots = workers
            .values()
            .map(|worker| WorkerSnapshot::from(worker))
            .collect::<Vec<_>>();
        scheduler.update_worker_state(&worker_snapshots);

        let result = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }
    }

    #[test]
    fn test_linear_scheduler_with_priority_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[(worker_1.clone(), 1), (worker_2.clone(), 1)]);

        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Add a low priority task
        let low_priority_task = create_schedulable_task(
            MockTaskBuilder::default()
                .with_task_id(1)
                .with_priority(1)
                .with_scheduling_strategy(SchedulingStrategy::Spread)
                .build(),
        );
        scheduler.enqueue_tasks(vec![low_priority_task]);

        // Add a high priority task
        let high_priority_task = create_schedulable_task(
            MockTaskBuilder::default()
                .with_priority(100)
                .with_task_id(2)
                .with_scheduling_strategy(SchedulingStrategy::Spread)
                .build(),
        );
        scheduler.enqueue_tasks(vec![high_priority_task]);

        // The high priority task should be scheduled first
        let result = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        assert_eq!(result[0].task.task_id(), 2);
    }

    #[test]
    fn test_linear_scheduler_with_no_workers() {
        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&HashMap::new());

        let tasks = vec![
            create_spread_task(Some(1)),
            create_worker_affinity_task(&Arc::from("worker1"), true, Some(2)),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);
    }
}
