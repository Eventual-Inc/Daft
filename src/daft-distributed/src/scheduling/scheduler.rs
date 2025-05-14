use std::collections::{HashMap, HashSet};

use super::{
    dispatcher::SchedulableTask,
    task::{SchedulingStrategy, Task, TaskId},
    worker::{Worker, WorkerId},
};

#[allow(dead_code)]
pub(crate) struct ScheduleResult<T: Task> {
    pub scheduled_tasks: Vec<(WorkerId, SchedulableTask<T>)>,
    pub unscheduled_tasks: Vec<SchedulableTask<T>>,
}

#[allow(dead_code)]
pub(crate) trait Scheduler<T: Task, W: Worker>: Send + Sync {
    fn update_state(&mut self, workers: &HashMap<WorkerId, W>);
    fn schedule_tasks(&mut self, tasks: Vec<SchedulableTask<T>>) -> ScheduleResult<T>;
}

#[allow(dead_code)]
struct WorkerSnapshot {
    worker_id: WorkerId,
    num_cpus: usize,
    active_task_ids: HashSet<TaskId>,
}

#[allow(dead_code)]
impl WorkerSnapshot {
    fn new(worker: &impl Worker) -> Self {
        Self {
            worker_id: worker.id().clone(),
            active_task_ids: worker.active_task_ids(),
            num_cpus: worker.num_cpus(),
        }
    }
}

#[allow(dead_code)]
pub(crate) struct DefaultScheduler {
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
}

#[allow(dead_code)]
impl DefaultScheduler {
    pub fn new() -> Self {
        Self {
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

    fn try_schedule_task<T: Task>(&self, task: &SchedulableTask<T>) -> Option<WorkerId> {
        match task.strategy() {
            SchedulingStrategy::Spread => self.try_schedule_spread_task(),
            SchedulingStrategy::NodeAffinity { node_id, soft } => match soft {
                true => self.try_schedule_soft_node_affinity_task(node_id),
                false => self.try_schedule_hard_node_affinity_task(node_id),
            },
        }
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
            let worker_id = self.try_schedule_task(&task);
            if let Some(worker_id) = worker_id {
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

impl<T: Task, W: Worker> Scheduler<T, W> for LinearScheduler {
    fn update_state(&mut self, _workers: &HashMap<WorkerId, W>) {
        todo!("FLOTILLA_MS1: Implement updating state for linear scheduler")
    }

    fn schedule_tasks(&mut self, _tasks: Vec<SchedulableTask<T>>) -> ScheduleResult<T> {
        todo!("FLOTILLA_MS1: Implement scheduling tasks for linear scheduler")
    }
}
