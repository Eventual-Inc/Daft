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

use common_error::DaftResult;
use common_partitioning::{Partition, PartitionRef};
use uuid::Uuid;

use super::{
    task::{SchedulingStrategy, SwordfishTask, SwordfishTaskResultHandle, Task, TaskId},
    worker::{Worker, WorkerId, WorkerManager, WorkerManagerFactory},
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

pub(crate) struct MockTask {
    task_id: String,
    scheduling_strategy: SchedulingStrategy,
    task_result: PartitionRef,
    cancel_marker: Option<Arc<AtomicBool>>,
    sleep_duration: Option<Duration>,
}

/// A builder pattern implementation for creating MockTask instances
pub(crate) struct MockTaskBuilder {
    task_id: Option<String>,
    scheduling_strategy: SchedulingStrategy,
    task_result: PartitionRef,
    cancel_marker: Option<Arc<AtomicBool>>,
    sleep_duration: Option<Duration>,
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

    /// Build the MockTask
    pub fn build(self) -> MockTask {
        MockTask {
            task_id: self.task_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            scheduling_strategy: self.scheduling_strategy,
            task_result: self.task_result,
            cancel_marker: self.cancel_marker,
            sleep_duration: self.sleep_duration,
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
}

impl MockTaskResultHandle {
    fn new(
        result: PartitionRef,
        worker_manager: MockWorkerManager,
        worker_id: WorkerId,
        sleep_duration: Option<Duration>,
        cancel_marker: Option<Arc<AtomicBool>>,
    ) -> Self {
        Self {
            result,
            worker_manager,
            worker_id,
            sleep_duration,
            cancel_marker,
        }
    }
}

#[async_trait::async_trait]
impl SwordfishTaskResultHandle for MockTaskResultHandle {
    async fn get_result(&mut self) -> DaftResult<Vec<PartitionRef>> {
        if let Some(sleep_duration) = self.sleep_duration {
            tokio::time::sleep(sleep_duration).await;
        }
        self.worker_manager.mark_task_finished(TaskId::default(), self.worker_id.clone());
        Ok(vec![self.result.clone()])
    }
}

impl Drop for MockTaskResultHandle {
    fn drop(&mut self) {
        if let Some(cancel_marker) = self.cancel_marker.take() {
            cancel_marker.store(true, Ordering::SeqCst);
        }
        self.worker_manager.mark_task_finished(TaskId::default(), self.worker_id.clone());
    }
}

/// A mock implementation of the WorkerManager trait for testing
#[derive(Clone)]
pub struct MockWorkerManager {
    workers: Arc<Mutex<HashMap<WorkerId, MockWorker>>>,
}

impl MockWorkerManager {
    pub fn new() -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_worker(&mut self, worker_id: WorkerId, num_cpus: usize) -> DaftResult<()> {
        let mut workers = self.workers.lock().unwrap();
        workers.insert(worker_id.clone(), MockWorker::new(worker_id, num_cpus));
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct MockWorker {
    worker_id: WorkerId,
    num_cpus: usize,
    num_active_tasks: usize,
    active_task_ids: HashSet<TaskId>,
}

impl MockWorker {
    pub fn new(worker_id: WorkerId, num_cpus: usize) -> Self {
        Self {
            worker_id,
            num_cpus,
            num_active_tasks: 0,
            active_task_ids: HashSet::new(),
        }
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
        self.active_task_ids.clone()
    }
}

impl WorkerManager for MockWorkerManager {
    type Worker = MockWorker;

    fn submit_task_to_worker(
        &mut self,
        task: Box<dyn Task>,
        worker_id: WorkerId,
    ) -> Box<dyn SwordfishTaskResultHandle> {
        // First, get the task as a MockTask
        let task = *task.into_any().downcast::<MockTask>().unwrap();
        
        // Update the worker's active task count
        let mut workers = self.workers.lock().unwrap();
        if let Some(worker) = workers.get_mut(&worker_id) {
            worker.num_active_tasks += 1;
            worker.active_task_ids.insert(task.task_id.clone());
        }
        
        Box::new(MockTaskResultHandle::new(
            task.task_result,
            self.clone(),
            worker_id,
            task.sleep_duration,
            task.cancel_marker,
        ))
    }

    fn mark_task_finished(&mut self, task_id: TaskId, worker_id: WorkerId) {
        let mut workers = self.workers.lock().unwrap();
        if let Some(worker) = workers.get_mut(&worker_id) {
            worker.num_active_tasks = worker.num_active_tasks.saturating_sub(1);
            worker.active_task_ids.remove(&task_id);
        }
    }

    fn workers(&self) -> &HashMap<WorkerId, Self::Worker> {
        // This is not ideal because we can't return a reference to the mutex-guarded map
        // For testing purposes, we'll just panic in this method - it shouldn't be called directly
        panic!("workers() method not implemented for MockWorkerManager");
    }

    fn total_available_cpus(&self) -> usize {
        let workers = self.workers.lock().unwrap();
        workers.values().map(|w| w.num_cpus).sum()
    }

    fn try_autoscale(&self, _num_workers: usize) -> DaftResult<()> {
        // No-op for mock implementation
        Ok(())
    }

    fn shutdown(&mut self) -> DaftResult<()> {
        let mut workers = self.workers.lock().unwrap();
        workers.clear();
        Ok(())
    }
}
