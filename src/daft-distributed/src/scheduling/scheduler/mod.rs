use std::{cmp::Ordering, collections::HashSet};

use super::{
    task::{SchedulingStrategy, Task, TaskId},
    worker::{Worker, WorkerId},
};
use crate::utils::channel::OneshotSender;

mod default;
mod linear;
mod scheduler_actor;

use common_error::DaftResult;
use common_partitioning::PartitionRef;
pub(crate) use scheduler_actor::{spawn_default_scheduler_actor, SchedulerHandle, SubmittedTask};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
pub(super) trait Scheduler<T: Task>: Send + Sync {
    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]);
    fn enqueue_tasks(&mut self, tasks: Vec<SchedulableTask<T>>);
    fn get_schedulable_tasks(&mut self) -> Vec<ScheduledTask<T>>;
    fn num_pending_tasks(&self) -> usize;
}

#[allow(dead_code)]
pub(super) struct SchedulableTask<T: Task> {
    task: T,
    result_tx: OneshotSender<DaftResult<PartitionRef>>,
    cancel_token: CancellationToken,
}

#[allow(dead_code)]
impl<T: Task> SchedulableTask<T> {
    pub fn new(
        task: T,
        result_tx: OneshotSender<DaftResult<PartitionRef>>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            task,
            result_tx,
            cancel_token,
        }
    }

    pub fn strategy(&self) -> &SchedulingStrategy {
        self.task.strategy()
    }

    #[allow(dead_code)]
    pub fn priority(&self) -> u32 {
        self.task.priority()
    }

    #[allow(dead_code)]
    pub fn task_id(&self) -> &TaskId {
        self.task.task_id()
    }

    pub fn into_inner(
        self,
    ) -> (
        T,
        OneshotSender<DaftResult<PartitionRef>>,
        CancellationToken,
    ) {
        (self.task, self.result_tx, self.cancel_token)
    }
}

impl<T: Task> PartialEq for SchedulableTask<T> {
    fn eq(&self, other: &Self) -> bool {
        self.task.task_id() == other.task.task_id()
    }
}

impl<T: Task> Eq for SchedulableTask<T> {}

impl<T: Task> PartialOrd for SchedulableTask<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Task> Ord for SchedulableTask<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.task.priority().cmp(&other.task.priority())
    }
}

#[allow(dead_code)]
pub(super) struct ScheduledTask<T: Task> {
    pub task: SchedulableTask<T>,
    pub worker_id: WorkerId,
}

#[allow(dead_code)]
impl<T: Task> ScheduledTask<T> {
    pub fn new(task: SchedulableTask<T>, worker_id: WorkerId) -> Self {
        Self { task, worker_id }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(super) struct WorkerSnapshot {
    worker_id: WorkerId,
    num_cpus: usize,
    active_task_ids: HashSet<TaskId>,
}

#[allow(dead_code)]
impl WorkerSnapshot {
    pub fn new(worker_id: WorkerId, num_cpus: usize, active_task_ids: HashSet<TaskId>) -> Self {
        Self {
            worker_id,
            num_cpus,
            active_task_ids,
        }
    }

    pub fn from_worker(worker: &impl Worker) -> Self {
        Self::new(
            worker.id().clone(),
            worker.num_cpus(),
            worker.active_task_ids(),
        )
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::{
        any::Any,
        collections::HashMap,
        sync::{atomic::AtomicBool, Arc, Mutex},
        time::Duration,
    };

    use common_error::DaftError;
    use common_partitioning::Partition;
    use uuid::Uuid;

    use super::*;
    use crate::scheduling::{task::TaskResultHandle, worker::WorkerManager};
    #[derive(Debug)]
    pub(crate) struct MockPartition {
        num_rows: usize,
        size_bytes: usize,
    }

    impl MockPartition {
        pub fn new(num_rows: usize, size_bytes: usize) -> Self {
            Self {
                num_rows,
                size_bytes,
            }
        }
    }

    impl Partition for MockPartition {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn size_bytes(&self) -> DaftResult<Option<usize>> {
            Ok(Some(self.size_bytes))
        }

        fn num_rows(&self) -> DaftResult<usize> {
            Ok(self.num_rows)
        }
    }

    pub(crate) fn create_mock_partition_ref(num_rows: usize, size_bytes: usize) -> PartitionRef {
        Arc::new(MockPartition::new(num_rows, size_bytes))
    }

    #[derive(Debug)]
    pub(crate) struct MockTask {
        task_id: TaskId,
        priority: u32,
        scheduling_strategy: SchedulingStrategy,
        task_result: PartitionRef,
        cancel_notifier: Option<OneshotSender<()>>,
        sleep_duration: Option<std::time::Duration>,
        failure: Option<MockTaskFailure>,
    }

    #[derive(Debug, Clone)]
    pub(crate) enum MockTaskFailure {
        Error(String),
        Panic(String),
    }

    /// A builder pattern implementation for creating MockTask instances
    pub(crate) struct MockTaskBuilder {
        task_id: TaskId,
        priority: u32,
        scheduling_strategy: SchedulingStrategy,
        task_result: PartitionRef,
        cancel_notifier: Option<OneshotSender<()>>,
        sleep_duration: Option<Duration>,
        failure: Option<MockTaskFailure>,
    }

    impl Default for MockTaskBuilder {
        fn default() -> Self {
            Self::new(create_mock_partition_ref(100, 100))
        }
    }

    impl MockTaskBuilder {
        /// Create a new MockTaskBuilder with required parameters
        pub fn new(partition_ref: PartitionRef) -> Self {
            Self {
                task_id: Arc::from(Uuid::new_v4().to_string()),
                priority: 0,
                scheduling_strategy: SchedulingStrategy::Spread,
                task_result: partition_ref,
                cancel_notifier: None,
                sleep_duration: None,
                failure: None,
            }
        }

        pub fn with_priority(mut self, priority: u32) -> Self {
            self.priority = priority;
            self
        }

        pub fn with_scheduling_strategy(mut self, scheduling_strategy: SchedulingStrategy) -> Self {
            self.scheduling_strategy = scheduling_strategy;
            self
        }

        /// Set a custom task ID (defaults to a UUID if not specified)
        pub fn with_task_id(mut self, task_id: TaskId) -> Self {
            self.task_id = task_id;
            self
        }

        /// Set a cancel marker
        pub fn with_cancel_notifier(mut self, cancel_notifier: OneshotSender<()>) -> Self {
            self.cancel_notifier = Some(cancel_notifier);
            self
        }

        /// Set a sleep duration
        pub fn with_sleep_duration(mut self, sleep_duration: Duration) -> Self {
            self.sleep_duration = Some(sleep_duration);
            self
        }

        pub fn with_failure(mut self, failure: MockTaskFailure) -> Self {
            self.failure = Some(failure);
            self
        }

        /// Build the MockTask
        pub fn build(self) -> MockTask {
            MockTask {
                task_id: self.task_id,
                priority: self.priority,
                scheduling_strategy: self.scheduling_strategy,
                task_result: self.task_result,
                cancel_notifier: self.cancel_notifier,
                sleep_duration: self.sleep_duration,
                failure: self.failure,
            }
        }
    }

    impl Task for MockTask {
        fn priority(&self) -> u32 {
            self.priority
        }

        fn task_id(&self) -> &TaskId {
            &self.task_id
        }

        fn strategy(&self) -> &SchedulingStrategy {
            &self.scheduling_strategy
        }
    }

    /// A mock implementation of the SwordfishTaskResultHandle trait for testing
    pub(crate) struct MockTaskResultHandle {
        result: PartitionRef,
        worker_manager: MockWorkerManager,
        worker_id: WorkerId,
        sleep_duration: Option<Duration>,
        cancel_notifier: Option<OneshotSender<()>>,
        failure: Option<MockTaskFailure>,
    }

    impl MockTaskResultHandle {
        fn new(
            result: PartitionRef,
            worker_manager: MockWorkerManager,
            worker_id: WorkerId,
            sleep_duration: Option<Duration>,
            cancel_notifier: Option<OneshotSender<()>>,
            failure: Option<MockTaskFailure>,
        ) -> Self {
            Self {
                result,
                worker_manager,
                worker_id,
                sleep_duration,
                cancel_notifier,
                failure,
            }
        }
    }

    impl TaskResultHandle for MockTaskResultHandle {
        async fn get_result(&mut self) -> DaftResult<PartitionRef> {
            if let Some(sleep_duration) = self.sleep_duration {
                tokio::time::sleep(sleep_duration).await;
            }
            if let Some(failure) = &self.failure {
                match failure {
                    MockTaskFailure::Error(error_message) => {
                        return Err(DaftError::InternalError(error_message.clone()));
                    }
                    MockTaskFailure::Panic(error_message) => {
                        panic!("{}", error_message);
                    }
                }
            }
            Ok(self.result.clone())
        }
    }
    /// A mock implementation of the WorkerManager trait for testing
    #[derive(Clone)]
    pub struct MockWorkerManager {
        workers: HashMap<WorkerId, MockWorker>,
    }

    impl MockWorkerManager {
        pub fn new() -> Self {
            Self {
                workers: HashMap::new(),
            }
        }

        pub fn add_worker(&mut self, worker_id: WorkerId, num_cpus: usize) -> DaftResult<()> {
            self.workers
                .insert(worker_id.clone(), MockWorker::new(worker_id, num_cpus));
            Ok(())
        }
    }

    #[derive(Clone)]
    pub(crate) struct MockWorker {
        worker_id: WorkerId,
        num_cpus: usize,
        num_active_tasks: usize,
        active_task_ids: Arc<Mutex<HashSet<TaskId>>>,
        is_shutdown: Arc<AtomicBool>,
    }

    impl MockWorker {
        pub fn new(worker_id: WorkerId, num_cpus: usize) -> Self {
            Self {
                worker_id,
                num_cpus,
                num_active_tasks: 0,
                active_task_ids: Arc::new(Mutex::new(HashSet::new())),
                is_shutdown: Arc::new(AtomicBool::new(false)),
            }
        }

        pub fn mark_task_finished(&self, task_id: &TaskId) {
            self.active_task_ids.lock().unwrap().remove(task_id);
        }

        pub fn add_active_task(&self, task_id: TaskId) {
            self.active_task_ids.lock().unwrap().insert(task_id);
        }

        pub fn shutdown(&self) {
            self.is_shutdown
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl Worker for MockWorker {
        type Task = MockTask;
        type TaskResultHandle = MockTaskResultHandle;

        fn id(&self) -> &WorkerId {
            &self.worker_id
        }

        fn num_cpus(&self) -> usize {
            self.num_cpus
        }

        fn active_task_ids(&self) -> HashSet<TaskId> {
            self.active_task_ids.lock().unwrap().clone()
        }
    }

    impl WorkerManager for MockWorkerManager {
        type Worker = MockWorker;

        fn submit_tasks_to_workers(
            &self,
            total_tasks: usize,
            tasks_per_worker: HashMap<
                WorkerId,
                Vec<<<Self as WorkerManager>::Worker as Worker>::Task>,
            >,
        ) -> DaftResult<Vec<<<Self as WorkerManager>::Worker as Worker>::TaskResultHandle>>
        {
            let mut result = Vec::new();

            for (worker_id, tasks) in tasks_per_worker {
                for task in tasks {
                    // Update the worker's active task count
                    if let Some(worker) = self.workers.get(&worker_id) {
                        worker.add_active_task(task.task_id().clone());
                    }

                    result.push(MockTaskResultHandle::new(
                        task.task_result,
                        self.clone(),
                        worker_id.clone(),
                        task.sleep_duration,
                        task.cancel_notifier,
                        task.failure,
                    ));
                }
            }

            Ok(result)
        }

        fn mark_task_finished(&self, task_id: TaskId, worker_id: WorkerId) {
            if let Some(worker) = self.workers.get(&worker_id) {
                worker.mark_task_finished(&task_id);
            }
        }

        fn workers(&self) -> &HashMap<WorkerId, Self::Worker> {
            &self.workers
        }

        fn total_available_cpus(&self) -> usize {
            self.workers.values().map(|w| w.num_cpus).sum()
        }

        fn try_autoscale(&self, _num_workers: usize) -> DaftResult<()> {
            // No-op for mock implementation
            Ok(())
        }

        fn shutdown(&self) -> DaftResult<()> {
            self.workers.values().for_each(|w| w.shutdown());
            Ok(())
        }
    }
}
