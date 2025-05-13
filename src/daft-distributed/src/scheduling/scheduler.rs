use std::collections::{HashMap, HashSet};

use super::{
    dispatcher::SchedulableTask,
    task::{SchedulingStrategy, Task, TaskId},
    worker::{Worker, WorkerId},
};

pub(crate) struct ScheduleResult<T: Task> {
    pub scheduled_tasks: Vec<(WorkerId, SchedulableTask<T>)>,
    pub unscheduled_tasks: Vec<SchedulableTask<T>>,
}

pub(crate) trait Scheduler<T: Task, W: Worker>: Send + Sync {
    fn update_state(&mut self, workers: &HashMap<WorkerId, W>);
    fn schedule_tasks(&mut self, tasks: Vec<SchedulableTask<T>>) -> ScheduleResult<T>;
}

struct WorkerSnapshot {
    worker_id: WorkerId,
    num_cpus: usize,
    active_task_ids: HashSet<TaskId>,
}

impl WorkerSnapshot {
    fn new(worker: &impl Worker) -> Self {
        Self {
            worker_id: worker.id().clone(),
            active_task_ids: worker.active_task_ids().clone(),
            num_cpus: worker.num_cpus(),
        }
    }
}
pub(crate) struct DefaultScheduler {
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
}

impl DefaultScheduler {
    pub fn new() -> Self {
        Self {
            worker_snapshots: HashMap::new(),
        }
    }
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

    fn try_schedule_soft_node_affinity_task(&self, node_id: &str) -> Option<WorkerId> {
        if let Some(worker) = self.worker_snapshots.get(node_id) {
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

    fn try_schedule_hard_node_affinity_task(&self, node_id: &str) -> Option<WorkerId> {
        if let Some(worker) = self.worker_snapshots.get(node_id) {
            if worker.active_task_ids.len() < worker.num_cpus {
                return Some(worker.worker_id.clone());
            }
        }

        None
    }
}

impl<T: Task, W: Worker> Scheduler<T, W> for DefaultScheduler {
    fn update_state(&mut self, workers: &HashMap<WorkerId, W>) {
        self.worker_snapshots = workers
            .iter()
            .map(|(id, worker)| (id.clone(), WorkerSnapshot::new(worker)))
            .collect();
    }

    fn schedule_tasks(&mut self, tasks: Vec<SchedulableTask<T>>) -> ScheduleResult<T> {
        let mut scheduled_tasks: Vec<(WorkerId, SchedulableTask<T>)> = Vec::new();
        let mut unscheduled_tasks: Vec<SchedulableTask<T>> = Vec::new();

        for task in tasks {
            let worker_id = match task.strategy() {
                SchedulingStrategy::Spread => self.try_schedule_spread_task(),
                SchedulingStrategy::NodeAffinity { node_id, soft } => match soft {
                    true => self.try_schedule_soft_node_affinity_task(node_id),
                    false => self.try_schedule_hard_node_affinity_task(node_id),
                },
            };
            if let Some(worker_id) = worker_id {
                println!("scheduled task {} to worker {}", task.task_id(), worker_id);
                let task_id = task.task_id().to_string();
                scheduled_tasks.push((worker_id.clone(), task));
                self.worker_snapshots
                    .get_mut(&worker_id)
                    .unwrap()
                    .active_task_ids
                    .insert(task_id);
            } else {
                unscheduled_tasks.push(task);
            }
        }

        ScheduleResult {
            scheduled_tasks,
            unscheduled_tasks,
        }
    }
}

pub(crate) struct LinearScheduler {
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
}

impl LinearScheduler {
    pub fn new() -> Self {
        Self {
            worker_snapshots: HashMap::new(),
        }
    }
}

impl<T: Task, W: Worker> Scheduler<T, W> for LinearScheduler {
    fn update_state(&mut self, workers: &HashMap<WorkerId, W>) {
        self.worker_snapshots = workers
            .iter()
            .map(|(id, worker)| (id.clone(), WorkerSnapshot::new(worker)))
            .collect();
    }

    fn schedule_tasks(&mut self, tasks: Vec<SchedulableTask<T>>) -> ScheduleResult<T> {
        let mut scheduled_tasks: Vec<(WorkerId, SchedulableTask<T>)> = Vec::new();
        let mut unscheduled_tasks: Vec<SchedulableTask<T>> = Vec::new();

        // Check if any worker has running tasks
        let all_workers_idle = self
            .worker_snapshots
            .values()
            .all(|worker| worker.active_task_ids.is_empty());

        // Schedule at most one task if all workers are idle
        if all_workers_idle && !tasks.is_empty() {
            // Choose the first available worker
            if let Some((worker_id, _)) = self.worker_snapshots.iter().next() {
                let mut task_iter = tasks.into_iter();

                if let Some(task) = task_iter.next() {
                    scheduled_tasks.push((worker_id.clone(), task));
                }

                // Add remaining tasks to unscheduled
                unscheduled_tasks.extend(task_iter);
            } else {
                // No workers available
                unscheduled_tasks = tasks;
            }
        } else {
            // Workers are busy or no tasks to schedule
            unscheduled_tasks = tasks;
        }

        ScheduleResult {
            scheduled_tasks,
            unscheduled_tasks,
        }
    }
}
