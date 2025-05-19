use std::collections::{BinaryHeap, HashMap};

use super::{SchedulableTask, ScheduledTask, Scheduler, WorkerSnapshot};
use crate::scheduling::{
    task::{SchedulingStrategy, Task},
    worker::WorkerId,
};

#[allow(dead_code)]
pub(super) struct DefaultScheduler<T: Task> {
    pending_tasks: BinaryHeap<SchedulableTask<T>>,
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
}

impl<T: Task> Default for DefaultScheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl<T: Task> DefaultScheduler<T> {
    pub fn new() -> Self {
        Self {
            pending_tasks: BinaryHeap::new(),
            worker_snapshots: HashMap::new(),
        }
    }

    // Spread scheduling: Schedule tasks to the worker with the most available slots
    // TODO: Change the approach to instead spread based on tasks of the same 'type', i.e. from the same pipeline node.
    fn try_schedule_spread_task(&self) -> Option<WorkerId> {
        let mut worker_id = None;
        let mut max_available_slots = 0;

        for (id, worker) in &self.worker_snapshots {
            let available_slots = worker.num_cpus - worker.active_task_ids.len();
            if available_slots > max_available_slots {
                max_available_slots = available_slots;
                worker_id = Some(id.clone());
            }
        }
        worker_id
    }

    // Soft worker affinity scheduling: Schedule task to the worker if it has capacity
    // Otherwise, try to schedule to any worker with capacity
    fn try_schedule_soft_worker_affinity_task(&self, worker_id: &WorkerId) -> Option<WorkerId> {
        if let Some(worker) = self.worker_snapshots.get(worker_id) {
            if worker.active_task_ids.len() < worker.num_cpus {
                return Some(worker.worker_id.clone());
            }
        }
        let mut worker_id = None;
        for (id, slots) in &self.worker_snapshots {
            if slots.active_task_ids.len() < slots.num_cpus {
                worker_id = Some(id.clone());
                break;
            }
        }
        worker_id
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

impl<T: Task> Scheduler<T> for DefaultScheduler<T> {
    fn enqueue_tasks(&mut self, tasks: Vec<SchedulableTask<T>>) {
        self.pending_tasks.extend(tasks);
    }

    fn get_schedulable_tasks(&mut self) -> Vec<ScheduledTask<T>> {
        let mut scheduled = Vec::new();
        let mut unscheduled = Vec::new();
        while let Some(task) = self.pending_tasks.pop() {
            if let Some(worker_id) = self.try_schedule_task(&task) {
                self.worker_snapshots
                    .get_mut(&worker_id)
                    .unwrap()
                    .active_task_ids
                    .insert(task.task_id().clone());
                scheduled.push(ScheduledTask { task, worker_id });
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
        worker::tests::MockWorker,
    };
    #[test]
    fn test_default_scheduler_spread_scheduling() {
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
            create_spread_task(),
            create_spread_task(),
            create_spread_task(),
        ];

        // Enqueue and schedule tasks
        scheduler.enqueue_tasks(initial_tasks);
        let result = scheduler.get_schedulable_tasks();

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
        assert_eq!(worker_task_counts.get(&worker_1), None);

        // Add 3 more tasks with Spread strategy
        let second_round_tasks = vec![
            create_spread_task(),
            create_spread_task(),
            create_spread_task(),
        ];
        scheduler.enqueue_tasks(second_round_tasks);

        let result = scheduler.get_schedulable_tasks();

        // All tasks should be scheduled
        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);

        // Count tasks per worker for the second batch
        let mut worker_task_counts: HashMap<&WorkerId, usize> = HashMap::new();
        for scheduled_task in &result {
            *worker_task_counts
                .entry(&scheduled_task.worker_id)
                .or_insert(0) += 1;
        }

        // Verify distribution - each worker should have 1 task since they all have 1 slot available
        assert_eq!(*worker_task_counts.get(&worker_3).unwrap(), 1);
        assert_eq!(*worker_task_counts.get(&worker_2).unwrap(), 1);
        assert_eq!(*worker_task_counts.get(&worker_1).unwrap(), 1);

        // Add 3 more tasks with Spread strategy
        let third_round_tasks = vec![
            create_spread_task(),
            create_spread_task(),
            create_spread_task(),
        ];
        scheduler.enqueue_tasks(third_round_tasks);

        let result = scheduler.get_schedulable_tasks();

        // No tasks should be scheduled because all workers are at capacity
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 3);
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
            create_worker_affinity_task(&worker_1, true), // should go to worker 1
            create_worker_affinity_task(&worker_2, true), // should go to worker 2
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.get_schedulable_tasks();

        // 2 tasks should be scheduled
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &result {
            if let SchedulingStrategy::WorkerAffinity { worker_id, .. } =
                &scheduled_task.task.strategy()
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
            create_worker_affinity_task(&worker_1, true),
            create_worker_affinity_task(&worker_2, true),
            create_worker_affinity_task(&worker_3, true),
        ];

        scheduler.enqueue_tasks(tasks);
        let result = scheduler.get_schedulable_tasks();

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
            create_worker_affinity_task(&worker_1, false),
            create_worker_affinity_task(&worker_2, false),
            create_worker_affinity_task(&worker_3, false),
        ];

        scheduler.enqueue_tasks(tasks);
        let scheduled_tasks = scheduler.get_schedulable_tasks();

        // 3 tasks should be scheduled, 1 for each worker
        assert_eq!(scheduled_tasks.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &scheduled_tasks {
            if let SchedulingStrategy::WorkerAffinity { worker_id, .. } =
                &scheduled_task.task.strategy()
            {
                assert_eq!(scheduled_task.worker_id, *worker_id);
            } else {
                panic!("Task should have worker affinity strategy");
            }
        }

        // Create tasks again
        let tasks = vec![
            create_worker_affinity_task(&worker_1, false), // should not be scheduled
            create_worker_affinity_task(&worker_2, false),
            create_worker_affinity_task(&worker_3, false),
        ];

        scheduler.enqueue_tasks(tasks);
        let scheduled_tasks = scheduler.get_schedulable_tasks();

        // worker 1 should not be available, worker 2 should have 1 slot available, worker 3 should have 2 slots available
        assert_eq!(scheduled_tasks.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        for scheduled_task in &scheduled_tasks {
            if let SchedulingStrategy::WorkerAffinity { worker_id, .. } =
                &scheduled_task.task.strategy()
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
        let result = scheduler.get_schedulable_tasks();

        // Only 2 tasks should be scheduled (one per worker)
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 98);

        // Add a high-priority task
        let high_priority_task =
            create_schedulable_task(MockTaskBuilder::default().with_priority(100).build());
        scheduler.enqueue_tasks(vec![high_priority_task]);

        // The high-priority task should not be scheduled because worker1 is full
        let result = scheduler.get_schedulable_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 99);

        // Update scheduler state to add a new worker with 1 slot available
        let worker_3: WorkerId = Arc::from("worker3");
        let new_worker = MockWorker::new(worker_3.clone(), 1);
        let new_worker_snapshot = WorkerSnapshot::from_worker(&new_worker);
        scheduler.update_worker_state(&[new_worker_snapshot]);

        // The high-priority task should now be scheduled to the new worker
        let result = scheduler.get_schedulable_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 98);
        assert_eq!(result[0].worker_id, worker_3);
    }

    #[test]
    fn test_scheduling_with_empty_workers() {
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&HashMap::new());

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
