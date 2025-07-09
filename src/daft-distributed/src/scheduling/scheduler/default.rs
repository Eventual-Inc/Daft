use std::collections::{BinaryHeap, HashMap};

use super::{PendingTask, ScheduledTask, Scheduler, WorkerSnapshot};
use crate::scheduling::{
    task::{SchedulingStrategy, Task, TaskDetails},
    worker::WorkerId,
};

pub(super) struct DefaultScheduler<T: Task> {
    pending_tasks: BinaryHeap<PendingTask<T>>,
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
}

impl<T: Task> Default for DefaultScheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Task> DefaultScheduler<T> {
    pub fn new() -> Self {
        Self {
            pending_tasks: BinaryHeap::new(),
            worker_snapshots: HashMap::new(),
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

    fn try_schedule_task(&self, task: &PendingTask<T>) -> Option<WorkerId> {
        match task.strategy() {
            SchedulingStrategy::Spread => self.try_schedule_spread_task(&task.task),
            SchedulingStrategy::WorkerAffinity { worker_id, soft } => {
                self.try_schedule_worker_affinity_task(&task.task, worker_id, *soft)
            }
        }
    }
}

impl<T: Task> Scheduler<T> for DefaultScheduler<T> {
    fn enqueue_tasks(&mut self, tasks: Vec<PendingTask<T>>) {
        self.pending_tasks.extend(tasks);
    }

    // TODO: Currently, workers are never given more tasks than they can handle (based on resources)
    // However, this can cause the scheduler to have too many pending tasks, creating a bottleneck in scheduling.
    // Potentially, we should allow workers to maintain a backlog queue of tasks, and automatically run them when they have capacity.
    // Key thing is that this should be profiled and tested.
    fn schedule_tasks(&mut self) -> Vec<ScheduledTask<T>> {
        let mut scheduled = Vec::new();
        let mut unscheduled = Vec::new();
        while let Some(task) = self.pending_tasks.pop() {
            if let Some(worker_id) = self.try_schedule_task(&task) {
                self.worker_snapshots
                    .get_mut(&worker_id)
                    .expect("Worker should be present in DefaultScheduler")
                    .active_task_details
                    .insert(task.task_context(), TaskDetails::from(&task.task));
                scheduled.push(ScheduledTask::new(task, worker_id));
            } else {
                unscheduled.push(task);
            }
        }
        self.pending_tasks.extend(unscheduled);
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

    fn num_pending_tasks(&self) -> usize {
        self.pending_tasks.len()
    }

    fn get_autoscaling_request(&mut self) -> Option<usize> {
        // if there's no workers, we need to scale up by the number of pending tasks
        if self.worker_snapshots.is_empty() {
            return Some(self.pending_tasks.len());
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_resource_request::ResourceRequest;

    use super::*;
    use crate::scheduling::{
        scheduler::test_utils::{
            create_schedulable_task, create_spread_task, create_worker_affinity_task,
            setup_scheduler, setup_workers,
        },
        tests::{MockTask, MockTaskBuilder},
        worker::tests::MockWorker,
    };

    #[test]
    fn test_default_scheduler_spread_scheduling_homogeneous_workers() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 3), // 3 slots available
            (worker_2.clone(), 3), // 3 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Spread strategy
        let initial_tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
        ];

        // Enqueue and schedule tasks
        scheduler.enqueue_tasks(initial_tasks);
        let result = scheduler.schedule_tasks();

        // All tasks should be scheduled because there is enough capacity
        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);

        // Count tasks per worker
        let mut worker_task_counts: HashMap<&WorkerId, usize> = HashMap::new();
        for scheduled_task in &result {
            *worker_task_counts
                .entry(&scheduled_task.worker_id)
                .or_insert(0) += 1;
        }

        // Verify distribution - worker3 should have 1 task (most slots), worker2 should have 1 task, worker1 should have 1 task
        assert_eq!(*worker_task_counts.get(&worker_3).unwrap(), 1);
        assert_eq!(*worker_task_counts.get(&worker_2).unwrap(), 1);
        assert_eq!(*worker_task_counts.get(&worker_1).unwrap(), 1);
    }

    #[test]
    fn test_default_scheduler_spread_scheduling_heterogeneous_workers() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 2), // 2 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Spread strategy
        let initial_tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
        ];

        // Enqueue and schedule tasks
        scheduler.enqueue_tasks(initial_tasks);
        let result = scheduler.schedule_tasks();

        // All tasks should be scheduled because there is enough capacity
        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);

        // Count tasks per worker
        let mut worker_task_counts: HashMap<&WorkerId, usize> = HashMap::new();
        for scheduled_task in &result {
            *worker_task_counts
                .entry(&scheduled_task.worker_id)
                .or_insert(0) += 1;
        }

        // Verify distribution - worker3 should have 2 tasks (most slots), worker2 should have 1 task, worker1 should have 0 tasks
        assert_eq!(*worker_task_counts.get(&worker_3).unwrap(), 2);
        assert_eq!(*worker_task_counts.get(&worker_2).unwrap(), 1);
        assert!(worker_task_counts.get(&worker_1).is_none());
    }

    #[test]
    fn test_default_scheduler_soft_node_affinity_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 1), // 1 slot available
            (worker_3.clone(), 2), // 2 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Node Affinity strategies
        let tasks = vec![
            create_worker_affinity_task(&worker_1, true, Some(1)), // should go to worker 1
            create_worker_affinity_task(&worker_2, true, Some(2)), // should go to worker 2
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        // 2 tasks should be scheduled
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &result {
            if let SchedulingStrategy::WorkerAffinity { worker_id, .. } =
                &scheduled_task.task().strategy()
            {
                assert_eq!(scheduled_task.worker_id, *worker_id);
            }
        }

        // Create tasks again, now the worker snapshots are:
        // worker1: 0 slots available
        // worker2: 0 slots available
        // worker3: 2 slots available
        // Regardless of which worker the task is affinity to, it should go to worker 3
        let tasks = vec![
            create_worker_affinity_task(&worker_1, true, Some(3)),
            create_worker_affinity_task(&worker_2, true, Some(4)),
            create_worker_affinity_task(&worker_3, true, Some(5)),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        // Only 2 tasks should be scheduled, because worker 3 has 2 slots available
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        for scheduled_task in &result {
            assert_eq!(scheduled_task.worker_id, worker_3);
        }
    }

    #[test]
    fn test_default_scheduler_hard_node_affinity_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 2), // 2 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Node Affinity strategies
        let tasks = vec![
            create_worker_affinity_task(&worker_1, false, Some(1)),
            create_worker_affinity_task(&worker_2, false, Some(2)),
            create_worker_affinity_task(&worker_3, false, Some(3)),
        ];

        scheduler.enqueue_tasks(tasks);
        let scheduled_tasks = scheduler.schedule_tasks();

        // 3 tasks should be scheduled, 1 for each worker
        assert_eq!(scheduled_tasks.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &scheduled_tasks {
            if let SchedulingStrategy::WorkerAffinity { worker_id, .. } =
                &scheduled_task.task().strategy()
            {
                assert_eq!(scheduled_task.worker_id, *worker_id);
            } else {
                panic!("Task should have worker affinity strategy");
            }
        }

        // Create tasks again
        let tasks = vec![
            create_worker_affinity_task(&worker_1, false, Some(1)), // should not be scheduled (worker busy)
            create_worker_affinity_task(&worker_2, false, Some(2)),
            create_worker_affinity_task(&worker_3, false, Some(3)),
        ];

        scheduler.enqueue_tasks(tasks);
        let scheduled_tasks = scheduler.schedule_tasks();

        // worker 1 should not be available, worker 2 should have 1 slot available, worker 3 should have 2 slots available
        assert_eq!(scheduled_tasks.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        for scheduled_task in &scheduled_tasks {
            if let SchedulingStrategy::WorkerAffinity { worker_id, .. } =
                &scheduled_task.task().strategy()
            {
                assert_eq!(scheduled_task.worker_id, *worker_id);
            } else {
                panic!("Task should have worker affinity strategy");
            }
        }
    }

    #[test]
    fn test_default_scheduler_with_priority_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 1), // 1 slot available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Add a lot of low priority tasks
        let tasks = (0..100)
            .map(|_| create_schedulable_task(MockTaskBuilder::default().with_priority(1).build()))
            .collect();

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        // Only 2 tasks should be scheduled (one per worker)
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 98);

        // Add a high-priority task
        let high_priority_task =
            create_schedulable_task(MockTaskBuilder::default().with_priority(100).build());
        scheduler.enqueue_tasks(vec![high_priority_task]);

        // The high-priority task should not be scheduled because worker1 is full
        let result = scheduler.schedule_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 99);

        // Update scheduler state to add a new worker with 1 slot available
        let worker_3: WorkerId = Arc::from("worker3");
        let new_worker = MockWorker::new(worker_3.clone(), 1.0, 0.0);
        let new_worker_snapshot = WorkerSnapshot::from(&new_worker);
        scheduler.update_worker_state(&[new_worker_snapshot]);

        // The high-priority task should now be scheduled to the new worker
        let result = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 98);
        assert_eq!(result[0].worker_id, worker_3);
    }

    #[test]
    fn test_default_scheduler_with_resource_request_scheduling_big_tasks_first() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 2), // 2 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        let tasks = vec![
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(3)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(3.0), None, None).unwrap(), // 3 CPUs
                    )
                    .build(),
            ),
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(2)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(2.0), None, None).unwrap(), // 2 CPUs
                    )
                    .build(),
            ),
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(1)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(1.0), None, None).unwrap(), // 1 CPU
                    )
                    .build(),
            ),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &result {
            if scheduled_task.worker_id == worker_1 {
                assert_eq!(scheduled_task.task().task_id(), 1);
            } else if scheduled_task.worker_id == worker_2 {
                assert_eq!(scheduled_task.task().task_id(), 2);
            } else if scheduled_task.worker_id == worker_3 {
                assert_eq!(scheduled_task.task().task_id(), 3);
            }
        }
    }

    // TODO: This test currently fails because the scheduler is currently not optimal, we should fix this by using a bin packing algorithm.
    // In this test case, we have 3 workers with 1, 2, and 3 slots available, and 3 tasks requesting 1, 2, and 3 CPUs.
    // In the ideal case, the scheduler should schedule the tasks in the following order:
    // 1. Task 1 (1 CPU) to worker 1 (1 slot available)
    // 2. Task 2 (2 CPUs) to worker 2 (2 slots available)
    // 3. Task 3 (3 CPUs) to worker 3 (3 slots available)
    // However, the scheduler currently schedules the tasks simply by picking the worker with the most available slots.
    // This results in the following schedule:
    // 1. Task 1 (1 CPU) to worker 3 (3 slots available)
    // 2. Task 2 (2 CPUs) to worker 2 or 3 (2 slots available)
    // 3. Task 3 (3 CPUs) is unscheduled (no worker has 3 slots available)
    #[test]
    #[ignore]
    fn test_default_scheduler_with_resource_request_scheduling_small_tasks_first() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 2), // 2 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        let tasks = vec![
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(1)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(1.0), None, None).unwrap(), // 1 CPU
                    )
                    .build(),
            ),
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(2)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(2.0), None, None).unwrap(), // 2 CPUs
                    )
                    .build(),
            ),
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(3)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(3.0), None, None).unwrap(), // 3 CPUs
                    )
                    .build(),
            ),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &result {
            if scheduled_task.worker_id == worker_1 {
                assert_eq!(scheduled_task.task().task_id(), 1);
            } else if scheduled_task.worker_id == worker_2 {
                assert_eq!(scheduled_task.task().task_id(), 2);
            } else if scheduled_task.worker_id == worker_3 {
                assert_eq!(scheduled_task.task().task_id(), 3);
            }
        }
    }

    #[test]
    fn test_scheduling_with_empty_workers() {
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&HashMap::new());

        let tasks = vec![
            create_spread_task(Some(1)),
            create_worker_affinity_task(&Arc::from("worker1"), true, Some(1)),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);
    }

    #[test]
    fn test_scheduling_with_more_tasks_than_workers() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 1), // 1 slot available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create 5 tasks with Spread strategy - more than available workers
        let tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
            create_spread_task(Some(4)),
            create_spread_task(Some(5)),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        // Only 2 tasks should be scheduled (1 per worker)
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 3);

        // Count tasks per worker - each should have exactly 1
        let mut worker_task_counts: HashMap<&WorkerId, usize> = HashMap::new();
        for scheduled_task in &result {
            *worker_task_counts
                .entry(&scheduled_task.worker_id)
                .or_insert(0) += 1;
        }

        assert_eq!(*worker_task_counts.get(&worker_1).unwrap(), 1);
        assert_eq!(*worker_task_counts.get(&worker_2).unwrap(), 1);
    }

    #[test]
    fn test_default_scheduler_with_no_workers_can_autoscale() {
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&HashMap::new());

        let tasks = vec![create_spread_task(Some(1))];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.schedule_tasks();

        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        assert_eq!(scheduler.get_autoscaling_request(), Some(1));
    }
}
