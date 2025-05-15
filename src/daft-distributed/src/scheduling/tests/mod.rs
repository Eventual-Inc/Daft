mod dispatcher;
mod scheduler;

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use common_error::{DaftError, DaftResult};
use common_partitioning::{Partition, PartitionRef};
use uuid::Uuid;

use super::{
    dispatcher::{TaskDispatcherHandle, TaskDispatcherHandleRef},
    scheduler::Scheduler,
    task::{SchedulingStrategy, SwordfishTaskResultHandle, Task, TaskId},
    worker::{Worker, WorkerId, WorkerManager},
};
use crate::utils::{channel::OneshotSender, joinset::JoinSet};

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
    task_id: String,
    scheduling_strategy: SchedulingStrategy,
    task_result: PartitionRef,
    cancel_notifier: Option<OneshotSender<()>>,
    sleep_duration: Option<Duration>,
    failure: Option<MockTaskFailure>,
}

#[derive(Debug)]
pub(crate) enum MockTaskFailure {
    Error(String),
    Panic(String),
}

/// A builder pattern implementation for creating MockTask instances
pub(crate) struct MockTaskBuilder {
    task_id: Option<String>,
    scheduling_strategy: SchedulingStrategy,
    task_result: PartitionRef,
    cancel_notifier: Option<OneshotSender<()>>,
    sleep_duration: Option<Duration>,
    failure: Option<MockTaskFailure>,
}

impl MockTaskBuilder {
    /// Create a new MockTaskBuilder with required parameters
    pub fn new(scheduling_strategy: SchedulingStrategy, partition_ref: PartitionRef) -> Self {
        Self {
            task_id: None,
            scheduling_strategy,
            task_result: partition_ref,
            cancel_notifier: None,
            sleep_duration: None,
            failure: None,
        }
    }

    /// Set a custom task ID (defaults to a UUID if not specified)
    pub fn with_task_id(mut self, task_id: String) -> Self {
        self.task_id = Some(task_id);
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
            task_id: self.task_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            scheduling_strategy: self.scheduling_strategy,
            task_result: self.task_result,
            cancel_notifier: self.cancel_notifier,
            sleep_duration: self.sleep_duration,
            failure: self.failure,
        }
    }
}

impl Task for MockTask {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    fn strategy(&self) -> &SchedulingStrategy {
        &self.scheduling_strategy
    }
}

/// A mock implementation of the SwordfishTaskResultHandle trait for testing
struct MockTaskResultHandle {
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

#[async_trait::async_trait]
impl SwordfishTaskResultHandle for MockTaskResultHandle {
    async fn get_result(&mut self) -> DaftResult<Vec<PartitionRef>> {
        if let Some(sleep_duration) = self.sleep_duration {
            tokio::time::sleep(sleep_duration).await;
        }
        if let Some(failure) = self.failure.take() {
            match failure {
                MockTaskFailure::Error(error_message) => {
                    return Err(DaftError::InternalError(error_message));
                }
                MockTaskFailure::Panic(error_message) => {
                    panic!("{}", error_message);
                }
            }
        }
        self.worker_manager
            .mark_task_finished(TaskId::default(), self.worker_id.clone());
        Ok(vec![self.result.clone()])
    }

    fn cancel_callback(&mut self) -> DaftResult<()> {
        if let Some(cancel_notifier) = self.cancel_notifier.take() {
            let _ = cancel_notifier.send(());
        }
        self.worker_manager
            .mark_task_finished(TaskId::default(), self.worker_id.clone());
        Ok(())
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

    pub fn mark_task_finished(&self, task_id: TaskId) {
        self.active_task_ids.lock().unwrap().remove(&task_id);
    }

    pub fn add_active_task(&self, task_id: TaskId) {
        self.active_task_ids.lock().unwrap().insert(task_id);
    }

    pub fn shutdown(&self) {
        self.is_shutdown.store(true, Ordering::SeqCst);
    }
}

impl Worker for MockWorker {
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

    fn submit_task_to_worker(
        &self,
        task: Box<dyn Task>,
        worker_id: WorkerId,
    ) -> Box<dyn SwordfishTaskResultHandle> {
        // First, get the task as a MockTask
        let task = *task.into_any().downcast::<MockTask>().unwrap();

        // Update the worker's active task count
        if let Some(worker) = self.workers.get(&worker_id) {
            worker.add_active_task(task.task_id.clone());
        }

        Box::new(MockTaskResultHandle::new(
            task.task_result,
            self.clone(),
            worker_id,
            task.sleep_duration,
            task.cancel_notifier,
            task.failure,
        ))
    }

    fn mark_task_finished(&self, task_id: TaskId, worker_id: WorkerId) {
        if let Some(worker) = self.workers.get(&worker_id) {
            worker.mark_task_finished(task_id);
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

pub(crate) struct TestContext {
    joinset: JoinSet<DaftResult<()>>,
    handle: TaskDispatcherHandleRef<MockTask>,
}

impl TestContext {
    pub fn new(
        workers: Vec<(String, usize)>,
        scheduler: Box<dyn Scheduler<MockTask, MockWorker>>,
    ) -> DaftResult<Self> {
        let mut worker_manager = MockWorkerManager::new();
        for (name, num_workers) in workers {
            worker_manager.add_worker(name, num_workers)?;
        }
        let task_dispatcher = crate::scheduling::dispatcher::TaskDispatcher::new_with_scheduler(
            Arc::new(worker_manager),
            scheduler,
        );
        let mut joinset = JoinSet::new();
        let handle = crate::scheduling::dispatcher::TaskDispatcher::spawn_task_dispatcher(
            task_dispatcher,
            &mut joinset,
        );
        Ok(Self { joinset, handle })
    }

    pub fn handle(&self) -> &TaskDispatcherHandleRef<MockTask> {
        &self.handle
    }

    pub fn spawn_on_joinset(
        &mut self,
        future: impl Future<Output = DaftResult<()>> + Send + 'static,
    ) {
        self.joinset.spawn(future);
    }

    pub async fn cleanup(mut self) -> DaftResult<()> {
        drop(self.handle);
        while let Some(result) = self.joinset.join_next().await {
            result.map_err(|e| DaftError::External(e.into()))??;
        }
        Ok(())
    }
}
