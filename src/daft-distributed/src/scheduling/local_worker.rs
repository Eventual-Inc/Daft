//! Test-only worker that executes `SwordfishTask`s in-process via the local
//! execution pipeline, producing real `ExecutionStats` without needing Ray.
//!
//! This entire module is gated behind `#[cfg(all(test, not(feature = "python")))]`
//! at the parent `scheduling/mod.rs`, so it never compiles into production
//! builds. It relies on `daft_local_execution::testing::execute_local_plan`,
//! which is itself only compiled without the python feature.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{ExecutionStats, Input, SourceId};
use daft_micropartition::MicroPartition;

use super::{
    scheduler::WorkerSnapshot,
    task::{
        SwordfishTask, Task, TaskContext, TaskDetails, TaskResourceRequest, TaskResultHandle,
        TaskStatus,
    },
    worker::{Worker, WorkerId, WorkerManager},
};
use crate::pipeline_node::{MaterializedOutput, TaskOutput};

/// A worker that executes `SwordfishTask`s in-process via the local execution pipeline.
///
/// This avoids the need for a Ray cluster while still exercising the real local execution
/// code path, producing genuine `ExecutionStats` with real `StatSnapshot` data.
#[derive(Debug, Clone)]
pub struct LocalSwordfishWorker {
    worker_id: WorkerId,
    total_num_cpus: f64,
    active_task_details: Arc<Mutex<HashMap<TaskContext, TaskDetails>>>,
}

impl LocalSwordfishWorker {
    pub fn new(worker_id: WorkerId) -> Self {
        Self {
            worker_id,
            total_num_cpus: 1.0,
            active_task_details: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_active_task(&self, task: &SwordfishTask) {
        self.active_task_details
            .lock()
            .unwrap()
            .insert(task.task_context(), TaskDetails::from(task));
    }

    pub fn mark_task_finished(&self, task_context: TaskContext) {
        self.active_task_details
            .lock()
            .unwrap()
            .remove(&task_context);
    }
}

impl Worker for LocalSwordfishWorker {
    type Task = SwordfishTask;
    type TaskResultHandle = LocalSwordfishTaskResultHandle;

    fn id(&self) -> &WorkerId {
        &self.worker_id
    }

    fn active_task_details(&self) -> HashMap<TaskContext, TaskDetails> {
        self.active_task_details.lock().unwrap().clone()
    }

    fn total_num_cpus(&self) -> f64 {
        self.total_num_cpus
    }

    fn total_num_gpus(&self) -> f64 {
        0.0
    }

    fn active_num_cpus(&self) -> f64 {
        self.active_task_details.lock().unwrap().len() as f64
    }

    fn active_num_gpus(&self) -> f64 {
        0.0
    }
}

impl LocalSwordfishWorker {
    fn to_snapshot(&self) -> WorkerSnapshot {
        WorkerSnapshot::new(
            self.worker_id.clone(),
            self.total_num_cpus,
            0.0,
            self.active_task_details(),
        )
    }
}

/// Task result handle that executes the SwordfishTask's local plan in-process.
pub struct LocalSwordfishTaskResultHandle {
    task: SwordfishTask,
}

impl LocalSwordfishTaskResultHandle {
    pub fn new(task: SwordfishTask) -> Self {
        Self { task }
    }
}

impl TaskResultHandle for LocalSwordfishTaskResultHandle {
    fn task_context(&self) -> TaskContext {
        self.task.task_context()
    }

    fn get_result(&mut self) -> impl Future<Output = TaskStatus> + Send + 'static {
        let plan = self.task.plan();
        let config = self.task.config().clone();
        let inputs = self.task.inputs().clone();
        let psets = self.task.psets().clone();
        let task_id = self.task.task_context().task_id;

        async move {
            match execute_swordfish_task_locally(plan, config, inputs, psets, task_id).await {
                Ok((output, stats)) => TaskStatus::Success {
                    result: output,
                    stats,
                },
                Err(e) => TaskStatus::Failed { error: e },
            }
        }
    }

    fn cancel_callback(&mut self) {
        // No-op for local execution
    }
}

/// Execute a SwordfishTask's local plan in-process.
///
/// Converts the task's psets (partition refs containing MicroPartitions) into
/// `Input::InMemory` entries and runs the plan via the local execution pipeline.
/// Returns the appropriate `TaskOutput` variant (Materialized or ShuffleWrite).
async fn execute_swordfish_task_locally(
    plan: daft_local_plan::LocalPhysicalPlanRef,
    config: Arc<common_daft_config::DaftExecutionConfig>,
    task_inputs: HashMap<SourceId, Input>,
    psets: HashMap<SourceId, Vec<PartitionRef>>,
    task_id: u32,
) -> DaftResult<(TaskOutput, ExecutionStats)> {
    use daft_local_execution::testing::LocalPlanOutput;

    use crate::pipeline_node::{ShufflePartitionRef, ShuffleWriteOutput};

    // Merge psets into inputs. Psets contain PartitionRefs that are Arc<MicroPartition>
    // in local execution. Convert them to Input::InMemory.
    let mut inputs = task_inputs;
    for (source_id, partition_refs) in psets {
        let micro_partitions: Vec<Arc<MicroPartition>> = partition_refs
            .into_iter()
            .map(|pr| {
                pr.as_any()
                    .downcast_ref::<MicroPartition>()
                    .map(|mp| Arc::new(mp.clone()))
                    .ok_or_else(|| {
                        common_error::DaftError::InternalError(
                            "LocalSwordfishWorker: PartitionRef is not a MicroPartition"
                                .to_string(),
                        )
                    })
            })
            .collect::<DaftResult<Vec<_>>>()?;
        inputs.insert(source_id, Input::InMemory(micro_partitions));
    }

    let (output, stats) =
        daft_local_execution::testing::execute_local_plan(&plan, config, inputs).await?;

    let task_output = match output {
        LocalPlanOutput::Partitions(partitions) => {
            let partition_refs: Vec<PartitionRef> = partitions
                .into_iter()
                .map(|mp| Arc::new(mp) as PartitionRef)
                .collect();
            TaskOutput::Materialized(MaterializedOutput::new(
                partition_refs,
                "local-worker".into(),
                String::new(),
                task_id,
            ))
        }
        LocalPlanOutput::Shuffle(shuffle_partitions) => {
            // Each entry in shuffle_partitions corresponds to one output partition bucket.
            // Concatenate each bucket into a single MicroPartition for the ShufflePartitionRef.
            let shuffle_refs: Vec<ShufflePartitionRef> = shuffle_partitions
                .into_iter()
                .map(|bucket| {
                    if bucket.is_empty() {
                        let empty = MicroPartition::empty(None);
                        ShufflePartitionRef::Ray(Arc::new(empty) as PartitionRef)
                    } else if bucket.len() == 1 {
                        ShufflePartitionRef::Ray(bucket.into_iter().next().unwrap() as PartitionRef)
                    } else {
                        let parts: Vec<MicroPartition> =
                            bucket.into_iter().map(|mp| (*mp).clone()).collect();
                        let concatenated = MicroPartition::concat(parts)
                            .expect("Failed to concatenate shuffle bucket partitions");
                        ShufflePartitionRef::Ray(Arc::new(concatenated) as PartitionRef)
                    }
                })
                .collect();
            TaskOutput::ShuffleWrite(ShuffleWriteOutput::new(
                shuffle_refs,
                "local-worker".into(),
                task_id,
            ))
        }
    };

    Ok((task_output, stats))
}

/// A worker manager for LocalSwordfishWorkers.
#[derive(Clone)]
pub struct LocalSwordfishWorkerManager {
    workers: Arc<Mutex<HashMap<WorkerId, LocalSwordfishWorker>>>,
}

impl LocalSwordfishWorkerManager {
    pub fn new(workers: HashMap<WorkerId, LocalSwordfishWorker>) -> Self {
        Self {
            workers: Arc::new(Mutex::new(workers)),
        }
    }

    pub fn single_worker() -> Self {
        let worker_id: WorkerId = Arc::from("local-worker-0");
        let worker = LocalSwordfishWorker::new(worker_id.clone());
        let mut workers = HashMap::new();
        workers.insert(worker_id, worker);
        Self::new(workers)
    }
}

impl WorkerManager for LocalSwordfishWorkerManager {
    type Worker = LocalSwordfishWorker;

    fn submit_tasks_to_workers(
        &self,
        tasks_per_worker: HashMap<WorkerId, Vec<SwordfishTask>>,
    ) -> DaftResult<Vec<LocalSwordfishTaskResultHandle>> {
        let mut result = Vec::new();
        for (worker_id, tasks) in tasks_per_worker {
            for task in tasks {
                if let Some(worker) = self.workers.lock().unwrap().get(&worker_id) {
                    worker.add_active_task(&task);
                }
                result.push(LocalSwordfishTaskResultHandle::new(task));
            }
        }
        Ok(result)
    }

    fn mark_task_finished(&self, task_context: TaskContext, worker_id: WorkerId) {
        if let Some(worker) = self.workers.lock().unwrap().get(&worker_id) {
            worker.mark_task_finished(task_context);
        }
    }

    fn mark_worker_died(&self, worker_id: WorkerId) {
        self.workers.lock().unwrap().remove(&worker_id);
    }

    fn worker_snapshots(&self) -> DaftResult<Vec<WorkerSnapshot>> {
        Ok(self
            .workers
            .lock()
            .unwrap()
            .values()
            .map(|w| w.to_snapshot())
            .collect())
    }

    fn try_autoscale(&self, _resource_requests: Vec<TaskResourceRequest>) -> DaftResult<()> {
        Ok(())
    }

    fn shutdown(&self) -> DaftResult<()> {
        Ok(())
    }
}
