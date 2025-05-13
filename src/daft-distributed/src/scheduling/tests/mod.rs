mod dispatcher;
mod scheduler;

use std::{
    any::Any,
    collections::{HashMap, HashSet},
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
    task::{SchedulingStrategy, SwordfishTaskResultHandle, Task, TaskId},
    worker::{Worker, WorkerId, WorkerManager},
};

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

#[derive(Debug, Clone)]
pub(crate) struct MockTask {
    task_id: String,
    scheduling_strategy: SchedulingStrategy,
    task_result: PartitionRef,
    cancel_marker: Option<Arc<AtomicBool>>,
    sleep_duration: Option<Duration>,
    error_message: Option<String>,
}

/// A builder pattern implementation for creating MockTask instances
pub(crate) struct MockTaskBuilder {
    task_id: Option<String>,
    scheduling_strategy: SchedulingStrategy,
    task_result: PartitionRef,
    cancel_marker: Option<Arc<AtomicBool>>,
    sleep_duration: Option<Duration>,
    error_message: Option<String>,
}

impl MockTaskBuilder {
    /// Create a new MockTaskBuilder with required parameters
    pub fn new(scheduling_strategy: SchedulingStrategy, partition_ref: PartitionRef) -> Self {
        Self {
            task_id: None,
            scheduling_strategy,
            task_result: partition_ref,
            cancel_marker: None,
            sleep_duration: None,
            error_message: None,
        }
    }

    /// Set a custom task ID (defaults to a UUID if not specified)
    pub fn with_task_id(mut self, task_id: String) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Set a cancel marker
    pub fn with_cancel_marker(mut self, cancel_marker: Arc<AtomicBool>) -> Self {
        self.cancel_marker = Some(cancel_marker);
        self
    }

    /// Set a sleep duration
    pub fn with_sleep_duration(mut self, sleep_duration: Duration) -> Self {
        self.sleep_duration = Some(sleep_duration);
        self
    }

    pub fn with_error(mut self) -> Self {
        self.error_message = Some("test error".to_string());
        self
    }

    /// Build the MockTask
    pub fn build(self) -> MockTask {
        MockTask {
            task_id: self.task_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            scheduling_strategy: self.scheduling_strategy,
            task_result: self.task_result,
            cancel_marker: self.cancel_marker,
            sleep_duration: self.sleep_duration,
            error_message: self.error_message,
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
    cancel_marker: Option<Arc<AtomicBool>>,
    error_message: Option<String>,
}

impl MockTaskResultHandle {
    fn new(
        result: PartitionRef,
        worker_manager: MockWorkerManager,
        worker_id: WorkerId,
        sleep_duration: Option<Duration>,
        cancel_marker: Option<Arc<AtomicBool>>,
        error_message: Option<String>,
    ) -> Self {
        Self {
            result,
            worker_manager,
            worker_id,
            sleep_duration,
            cancel_marker,
            error_message,
        }
    }
}

#[async_trait::async_trait]
impl SwordfishTaskResultHandle for MockTaskResultHandle {
    async fn get_result(&mut self) -> DaftResult<Vec<PartitionRef>> {
        if let Some(sleep_duration) = self.sleep_duration {
            tokio::time::sleep(sleep_duration).await;
        }
        if let Some(error_message) = self.error_message.take() {
            return Err(DaftError::InternalError(error_message));
        }
        self.worker_manager
            .mark_task_finished(TaskId::default(), self.worker_id.clone());
        Ok(vec![self.result.clone()])
    }
}

impl Drop for MockTaskResultHandle {
    fn drop(&mut self) {
        if let Some(cancel_marker) = self.cancel_marker.take() {
            cancel_marker.store(true, Ordering::SeqCst);
        }
        self.worker_manager
            .mark_task_finished(TaskId::default(), self.worker_id.clone());
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
            task.cancel_marker,
            task.error_message,
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
