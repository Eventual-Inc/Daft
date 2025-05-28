use std::collections::{BinaryHeap, HashMap};

use super::{SchedulableTask, ScheduledTask, Scheduler, WorkerSnapshot};
use crate::scheduling::{
    task::{SchedulingStrategy, Task},
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

    // Spread scheduling: Schedule task to the first worker with available slots
    fn try_schedule_spread_task(&self) -> Option<WorkerId> {
        self.worker_snapshots
            .iter()
            .find(|(_, worker)| worker.active_task_ids.len() < worker.num_cpus)
            .map(|(id, _)| id.clone())
    }

    // Soft worker affinity scheduling: Schedule task to the worker if it has capacity
    // Otherwise, try to schedule to any worker with capacity
    fn try_schedule_soft_worker_affinity_task(&self, worker_id: &WorkerId) -> Option<WorkerId> {
        if let Some(worker) = self.worker_snapshots.get(worker_id) {
            if worker.active_task_ids.len() < worker.num_cpus {
                return Some(worker.worker_id.clone());
            }
        }
        self.worker_snapshots
            .iter()
            .find(|(_, worker)| worker.active_task_ids.len() < worker.num_cpus)
            .map(|(id, _)| id.clone())
    }

    // Hard worker affinity scheduling: Schedule task to the worker if it has capacity
    // Otherwise, return None
    fn try_schedule_hard_worker_affinity_task(&self, worker_id: &WorkerId) -> Option<WorkerId> {
        if let Some(worker) = self.worker_snapshots.get(worker_id) {
            if worker.active_task_ids.len() < worker.num_cpus {
                return Some(worker.worker_id.clone());
            }
        }
        None
    }

    fn try_schedule_task(&self, task: &SchedulableTask<T>) -> Option<WorkerId> {
        match task.strategy() {
            SchedulingStrategy::Spread => self.try_schedule_spread_task(),
            SchedulingStrategy::WorkerAffinity { worker_id, soft } => match soft {
                true => self.try_schedule_soft_worker_affinity_task(worker_id),
                false => self.try_schedule_hard_worker_affinity_task(worker_id),
            },
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
            .any(|worker| !worker.active_task_ids.is_empty());

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
                    .active_task_ids
                    .insert(task.task_id().clone());
                scheduled.push(ScheduledTask { task, worker_id });
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
            create_spread_task(),
            create_spread_task(),
            create_spread_task(),
        ];

        // Enqueue and schedule tasks
        scheduler.enqueue_tasks(tasks);
        let result = scheduler.get_schedulable_tasks();

        // Only one task should be scheduled because of linear scheduling
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Try to schedule more tasks - should fail because one task is already running
        let result = scheduler.get_schedulable_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Update worker state to reflect that the workers are all idle
        let worker_snapshots = workers
            .values()
            .map(|worker| WorkerSnapshot::from_worker(worker))
            .collect::<Vec<_>>();
        scheduler.update_worker_state(&worker_snapshots);

        // Now we should be able to schedule another task
        let result = scheduler.get_schedulable_tasks();
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
            create_worker_affinity_task(&worker_1, true),
            create_worker_affinity_task(&worker_2, true),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.get_schedulable_tasks();

        // Only one task should be scheduled
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        // Verify the scheduled task went to its preferred worker
        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }

        let result = scheduler.get_schedulable_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        // Update worker state to reflect that the workers are all idle
        let worker_snapshots = workers
            .values()
            .map(|worker| WorkerSnapshot::from_worker(worker))
            .collect::<Vec<_>>();
        scheduler.update_worker_state(&worker_snapshots);

        let result = scheduler.get_schedulable_tasks();
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
            create_worker_affinity_task(&worker_1, false),
            create_worker_affinity_task(&worker_2, false),
            create_worker_affinity_task(&worker_3, false),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.get_schedulable_tasks();

        // Only one task should be scheduled
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Verify the scheduled task went to its preferred worker
        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }

        let result = scheduler.get_schedulable_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Update worker state to reflect that the workers are all idle
        let worker_snapshots = workers
            .values()
            .map(|worker| WorkerSnapshot::from_worker(worker))
            .collect::<Vec<_>>();
        scheduler.update_worker_state(&worker_snapshots);

        let result = scheduler.get_schedulable_tasks();
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
                .with_priority(1)
                .with_scheduling_strategy(SchedulingStrategy::Spread)
                .build(),
        );
        scheduler.enqueue_tasks(vec![low_priority_task]);

        // Add a high priority task
        let high_priority_task = create_schedulable_task(
            MockTaskBuilder::default()
                .with_priority(100)
                .with_scheduling_strategy(SchedulingStrategy::Spread)
                .build(),
        );
        scheduler.enqueue_tasks(vec![high_priority_task]);

        // The high priority task should be scheduled first
        let result = scheduler.get_schedulable_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        assert_eq!(result[0].task.priority(), 100);
    }

    #[test]
    fn test_linear_scheduler_with_no_workers() {
        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&HashMap::new());

        let tasks = vec![
            create_spread_task(),
            create_worker_affinity_task(&Arc::from("worker1"), true),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.get_schedulable_tasks();

        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);
    }
}
