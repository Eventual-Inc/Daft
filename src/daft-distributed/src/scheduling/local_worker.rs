//! Test-only worker that executes `SwordfishTask`s in-process via the local
//! execution pipeline, producing real `ExecutionStats` without needing Ray.
//!
//! Each `LocalSwordfishWorker` owns one `NativeExecutor` — the same type a
//! real Ray Flotilla worker process owns — so tasks with matching
//! `plan_fingerprint`s routed to the same worker share a pipeline,
//! `IOStatsRef`, and `RuntimeStatsManager`. This matches production Flotilla's
//! per-worker plan-sharing behavior and is the reason tests using this worker
//! can catch bugs in shared-state accounting (e.g. bytes_read counters).
//!
//! This module is `#[cfg(test)]` at the parent `scheduling/mod.rs`.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
};

use daft_common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_execution::testing::NativeExecutor;
use daft_local_plan::{ExecutionStats, Input, InputId, SourceId};
use daft_micropartition::MicroPartition;

use super::{
    scheduler::WorkerSnapshot,
    task::{
        SwordfishTask, Task, TaskContext, TaskDetails, TaskResourceRequest, TaskResultHandle,
        TaskStatus,
    },
    worker::{Worker, WorkerId, WorkerManager},
};
use crate::pipeline_node::MaterializedOutput;

/// A worker that executes `SwordfishTask`s in-process via a real `NativeExecutor`.
///
/// One `NativeExecutor` is owned per worker — matching real Ray Flotilla, where
/// each worker process has its own executor with a pipeline cache keyed by
/// `plan_fingerprint`. Tasks with matching fingerprints routed to this worker
/// share a local Swordfish pipeline (and all the state hanging off it).
#[derive(Clone)]
pub struct LocalSwordfishWorker {
    worker_id: WorkerId,
    active_task_details: Arc<Mutex<HashMap<TaskContext, TaskDetails>>>,
    /// Shared executor; all tasks routed to this worker run through it, so
    /// same-fingerprint tasks share a pipeline exactly as they would on a real
    /// Ray worker.
    executor: Arc<Mutex<NativeExecutor>>,
    /// Fresh `InputId` generator. Each task submitted gets a unique input_id so
    /// multiple same-fingerprint tasks can run concurrently on one pipeline.
    input_id_counter: Arc<AtomicU32>,
}

impl std::fmt::Debug for LocalSwordfishWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalSwordfishWorker")
            .field("worker_id", &self.worker_id)
            .finish_non_exhaustive()
    }
}

impl LocalSwordfishWorker {
    pub fn new(worker_id: WorkerId) -> Self {
        Self {
            worker_id,
            active_task_details: Arc::new(Mutex::new(HashMap::new())),
            // is_flotilla_worker=false: don't start a ShuffleFlightServer.
            // Tests that need shuffle support can revisit this.
            executor: Arc::new(Mutex::new(NativeExecutor::new(false, ""))),
            input_id_counter: Arc::new(AtomicU32::new(0)),
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

    fn next_input_id(&self) -> InputId {
        self.input_id_counter.fetch_add(1, Ordering::Relaxed)
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
        // Only used by the scheduler's `can_schedule_task` check. Returning 1.0
        // means the scheduler dispatches one task at a time per worker; the
        // `NativeExecutor`'s plan cache still retains the pipeline across tasks
        // because subsequent `run()` calls arrive before the previous task's
        // `try_finish` tears it down.
        1.0
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
            self.total_num_cpus(),
            0.0,
            self.active_task_details(),
        )
    }
}

/// Task result handle that runs a task through the worker's shared `NativeExecutor`.
pub struct LocalSwordfishTaskResultHandle {
    task: SwordfishTask,
    worker_id: WorkerId,
    executor: Arc<Mutex<NativeExecutor>>,
    input_id: InputId,
}

impl LocalSwordfishTaskResultHandle {
    fn new(task: SwordfishTask, worker: &LocalSwordfishWorker) -> Self {
        Self {
            task,
            worker_id: worker.worker_id.clone(),
            executor: worker.executor.clone(),
            input_id: worker.next_input_id(),
        }
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
        let context = self.task.context().clone();
        let task_id = self.task.task_context().task_id;
        let worker_id = self.worker_id.clone();
        let executor = self.executor.clone();
        let input_id = self.input_id;

        async move {
            match execute_swordfish_task_on_executor(
                executor, plan, config, context, inputs, psets, input_id, task_id, worker_id,
            )
            .await
            {
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

/// Run a `SwordfishTask` through a shared `NativeExecutor` — the same path a
/// real Ray Flotilla worker takes.
///
/// Converts `psets` (`PartitionRef`s wrapping `MicroPartition`s) into
/// `Input::InMemory` entries, then enqueues the task on the executor keyed by
/// `plan_fingerprint` (carried in `context`), drains the output stream, and
/// harvests per-input_id stats via `try_finish`.
#[allow(clippy::too_many_arguments)]
async fn execute_swordfish_task_on_executor(
    executor: Arc<Mutex<NativeExecutor>>,
    plan: daft_local_plan::LocalPhysicalPlanRef,
    config: Arc<common_daft_config::DaftExecutionConfig>,
    context: HashMap<String, String>,
    task_inputs: HashMap<SourceId, Input>,
    psets: HashMap<SourceId, Vec<PartitionRef>>,
    input_id: InputId,
    task_id: u32,
    worker_id: WorkerId,
) -> DaftResult<(MaterializedOutput, ExecutionStats)> {
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
                        daft_common_error::DaftError::InternalError(
                            "LocalSwordfishWorker: PartitionRef is not a MicroPartition"
                                .to_string(),
                        )
                    })
            })
            .collect::<DaftResult<Vec<_>>>()?;
        inputs.insert(source_id, Input::InMemory(micro_partitions));
    }

    let (fingerprint, run_fut) = {
        let mut exec = executor.lock().unwrap();
        exec.run(
            &plan,
            config,
            vec![], // subscribers
            Some(context),
            inputs,
            input_id,
            true, // maintain_order
        )?
    };
    let result = run_fut.await?;
    // Mirror the production Python flow: drain this input_id's output so the
    // pipeline has finished reading bytes before stats are harvested.
    let partitions = result.collect_partitions_for_testing().await;

    let stats = {
        let mut exec = executor.lock().unwrap();
        exec.try_finish(fingerprint, input_id)?
    }
    .await?;

    let partition_refs: Vec<PartitionRef> = partitions
        .into_iter()
        .map(|mp| Arc::new(mp) as PartitionRef)
        .collect();
    let materialized = MaterializedOutput::new(partition_refs, worker_id, String::new(), task_id);

    Ok((materialized, stats))
}

/// A worker manager for `LocalSwordfishWorker`s.
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
        let workers = self.workers.lock().unwrap();
        for (worker_id, tasks) in tasks_per_worker {
            let worker = workers.get(&worker_id).ok_or_else(|| {
                daft_common_error::DaftError::InternalError(format!(
                    "LocalSwordfishWorkerManager: unknown worker_id {worker_id}"
                ))
            })?;
            for task in tasks {
                worker.add_active_task(&task);
                result.push(LocalSwordfishTaskResultHandle::new(task, worker));
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
