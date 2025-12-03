//! Global process pool for UDF execution.
//!
//! This module provides a shared pool of Python worker processes that can execute
//! multiple UDFs. Workers are lazily spawned and cache UDF state for efficient reuse.

use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

#[cfg(feature = "python")]
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Mutex,
};

use common_error::{DaftError, DaftResult};
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime, RuntimeRef, RuntimeTask};
#[cfg(feature = "python")]
use daft_core::series::Series;
use daft_dsl::{ExprRef, operator_metrics::OperatorMetrics};
#[cfg(feature = "python")]
use daft_recordbatch::RecordBatch;
#[cfg(feature = "python")]
use pyo3::{Py, PyAny, Python, prelude::*};
#[cfg(feature = "python")]
use tokio::sync::{mpsc, oneshot, Notify};

#[cfg(feature = "python")]
use crate::STDOUT;

/// Global process pool instance
pub(crate) static PROCESS_POOL: OnceLock<Arc<ProcessPoolManager>> = OnceLock::new();

/// Get or initialize the global process pool
pub(crate) fn get_or_init_process_pool() -> &'static Arc<ProcessPoolManager> {
    PROCESS_POOL.get_or_init(|| Arc::new(ProcessPoolManager::new()))
}

/// Environment variable for configuring pool size
const POOL_SIZE_ENV_VAR: &str = "DAFT_UDF_POOL_SIZE";

fn get_default_pool_size() -> usize {
    if let Ok(val) = std::env::var(POOL_SIZE_ENV_VAR) {
        if let Ok(size) = val.parse::<usize>() {
            return size.max(1);
        }
    }
    get_compute_pool_num_threads()
}

/// A task to be executed by a pool worker
#[derive(Clone)]
pub(crate) struct UdfTask {
    /// Unique identifier for the UDF (typically the full function name)
    pub udf_name: Arc<str>,
    /// Serialized expression bytes (only needed for first task to a worker)
    pub expr_bytes: Arc<[u8]>,
    /// The expression for this UDF
    pub expr: ExprRef,
}

/// Message sent to a worker actor
#[cfg(feature = "python")]
enum WorkerMessage {
    /// Execute a UDF task
    Execute {
        task: UdfTask,
        input: RecordBatch,
        needs_expr_bytes: bool,
        response: oneshot::Sender<DaftResult<(RecordBatch, Vec<String>, OperatorMetrics)>>,
    },
    /// Teardown a UDF
    TeardownUdf {
        udf_name: Arc<str>,
        response: oneshot::Sender<DaftResult<bool>>,
    },
    /// Shutdown the worker
    Shutdown,
}

/// Rust-side handle to a pool worker actor
#[cfg(feature = "python")]
pub(crate) struct PoolWorkerHandle {
    /// Channel to send messages to the worker actor
    sender: mpsc::UnboundedSender<WorkerMessage>,
    /// Runtime task for the worker actor
    runtime_task: Arc<Mutex<Option<RuntimeTask<()>>>>,
    /// Set of active UDF names in this worker
    active_udfs: Arc<Mutex<HashSet<Arc<str>>>>,
    /// Worker index for logging
    index: usize,
}

#[cfg(feature = "python")]
impl PoolWorkerHandle {
    /// Create a new pool worker handle and spawn the actor
    fn new(index: usize, runtime: RuntimeRef) -> DaftResult<Self> {
        // Create Python worker handle
        let py_handle = Python::attach(|py| {
            Ok::<Py<PyAny>, PyErr>(
                py.import(pyo3::intern!(py, "daft.execution.udf"))?
                    .getattr(pyo3::intern!(py, "PoolWorkerHandle"))?
                    .call0()?
                    .unbind(),
            )
        })?;

        // Create channel for worker messages
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let active_udfs = Arc::new(Mutex::new(HashSet::new()));

        // Spawn the worker actor on the compute runtime
        let runtime_task = runtime.spawn({
            let py_handle = Python::attach(|py| Ok::<_, DaftError>(py_handle.clone_ref(py)))?;
            let active_udfs = active_udfs.clone();
            let index = index;
            async move {
                worker_actor_loop(py_handle, &mut receiver, active_udfs, index).await;
            }
        });

        Ok(Self {
            sender,
            runtime_task: Arc::new(Mutex::new(Some(runtime_task))),
            active_udfs,
            index,
        })
    }

    /// Submit a UDF task for execution
    pub async fn submit(
        &self,
        task: &UdfTask,
        input: RecordBatch,
    ) -> DaftResult<(Series, OperatorMetrics)> {
        let (tx, rx) = oneshot::channel();

        // Check if this is a new UDF (needs initialization)
        let is_new_udf = {
            let active = self.active_udfs.lock().unwrap();
            !active.contains(&task.udf_name)
        };

        // Send task to worker
        let msg = WorkerMessage::Execute {
            task: task.clone(),
            input,
            needs_expr_bytes: is_new_udf,
            response: tx,
        };

        self.sender.send(msg).map_err(|_| {
            DaftError::InternalError("Worker actor channel closed".to_string())
        })?;

        // Wait for result
        let (batch, _stdout, metrics) = rx.await.map_err(|_| {
            DaftError::InternalError("Worker actor response channel closed".to_string())
        })??;

        // Track UDF as active if it's new
        if is_new_udf {
            let mut active = self.active_udfs.lock().unwrap();
            active.insert(task.udf_name.clone());
        }

        debug_assert!(
            batch.num_columns() == 1,
            "UDF should return a single column"
        );
        Ok((batch.get_column(0).clone(), metrics))
    }

    /// Teardown a UDF from this worker
    pub async fn teardown_udf(&self, udf_name: &str) -> DaftResult<bool> {
        let (tx, rx) = oneshot::channel();
        let udf_name: Arc<str> = udf_name.into();

        self.sender.send(WorkerMessage::TeardownUdf {
            udf_name: udf_name.clone(),
            response: tx,
        }).map_err(|_| {
            DaftError::InternalError("Worker actor channel closed".to_string())
        })?;

        let worker_exited = rx.await.map_err(|_| {
            DaftError::InternalError("Worker actor response channel closed".to_string())
        })??;

        // Remove from active UDFs
        if worker_exited {
            let mut active = self.active_udfs.lock().unwrap();
            active.remove(&udf_name);
        }

        Ok(worker_exited)
    }

    /// Get the number of active UDFs in this worker
    pub fn active_udfs_count(&self) -> usize {
        self.active_udfs.lock().unwrap().len()
    }

    /// Shutdown the worker actor
    pub async fn shutdown(self) -> DaftResult<()> {
        // Send shutdown message
        let _ = self.sender.send(WorkerMessage::Shutdown);
        // Drop sender to close channel
        drop(self.sender);
        // Wait for actor to finish
        let runtime_task = self.runtime_task.lock().unwrap().take();
        if let Some(task) = runtime_task {
            task.await?;
        }
        Ok(())
    }
}

/// Worker actor event loop
#[cfg(feature = "python")]
async fn worker_actor_loop(
    py_handle: Py<PyAny>,
    receiver: &mut mpsc::UnboundedReceiver<WorkerMessage>,
    active_udfs: Arc<Mutex<HashSet<Arc<str>>>>,
    index: usize,
) {
    while let Some(msg) = receiver.recv().await {
        match msg {
            WorkerMessage::Execute { task, input, needs_expr_bytes, response } => {
                let result = execute_udf_task(&py_handle, &task, input, needs_expr_bytes, index).await;
                let _ = response.send(result);
            }
            WorkerMessage::TeardownUdf { udf_name, response } => {
                let result = teardown_udf_on_worker(&py_handle, &udf_name, &active_udfs).await;
                let worker_exited = result.as_ref().copied().unwrap_or(false);
                let _ = response.send(result);
                // If worker exited (cache empty), break the loop
                if worker_exited {
                    break;
                }
            }
            WorkerMessage::Shutdown => {
                // Shutdown the Python worker
                let _ = Python::attach(|py| -> PyResult<()> {
                    py_handle.bind(py).call_method0(pyo3::intern!(py, "shutdown"))?;
                    Ok(())
                });
                break;
            }
        }
    }
}

/// Execute a UDF task on a Python worker
#[cfg(feature = "python")]
async fn execute_udf_task(
    py_handle: &Py<PyAny>,
    task: &UdfTask,
    input: RecordBatch,
    needs_expr_bytes: bool,
    worker_index: usize,
) -> DaftResult<(RecordBatch, Vec<String>, OperatorMetrics)> {
    use common_metrics::python::PyOperatorMetrics;
    use daft_recordbatch::python::PyRecordBatch;

    // Only send expr_bytes if this is a new UDF
    let expr_bytes = if needs_expr_bytes {
        Some(task.expr_bytes.as_ref())
    } else {
        None
    };

    // Execute the UDF
    let (result, stdout_lines, metrics) = Python::attach(|py| -> DaftResult<_> {
        let expr_bytes_py = expr_bytes.map(|b| pyo3::types::PyBytes::new(py, b));

        let (py_result, py_stdout_lines, py_metrics) = py_handle
            .bind(py)
            .call_method1(
                pyo3::intern!(py, "eval_input"),
                (
                    task.udf_name.as_ref(),
                    expr_bytes_py,
                    PyRecordBatch::from(input.clone()),
                ),
            )?
            .extract::<(PyRecordBatch, Vec<String>, PyOperatorMetrics)>()?;

        Ok((
            RecordBatch::from(py_result),
            py_stdout_lines,
            py_metrics.inner,
        ))
    })?;

    // Print stdout lines
    let label = format!("[Pool Worker #{}]", worker_index);
    for line in &stdout_lines {
        STDOUT.print(&label, line);
    }

    Ok((result, stdout_lines, metrics))
}

/// Teardown a UDF on a Python worker
#[cfg(feature = "python")]
async fn teardown_udf_on_worker(
    py_handle: &Py<PyAny>,
    udf_name: &str,
    active_udfs: &Arc<Mutex<HashSet<Arc<str>>>>,
) -> DaftResult<bool> {
    // Check if UDF is active
    let is_active = {
        let active = active_udfs.lock().unwrap();
        active.contains(udf_name)
    };

    if !is_active {
        return Ok(false);
    }

    // Call teardown on Python worker
    let worker_exited = Python::attach(|py| -> PyResult<bool> {
        py_handle
            .bind(py)
            .call_method1(pyo3::intern!(py, "teardown_udf"), (udf_name,))?
            .extract::<bool>()
    })
    .map_err(|e| DaftError::ComputeError(format!("Error calling teardown_udf: {}", e)))?;

    // Remove from active UDFs
    if worker_exited {
        let mut active = active_udfs.lock().unwrap();
        active.remove(udf_name);
    }

    Ok(worker_exited)
}

/// State for managing workers in the pool
#[cfg(feature = "python")]
struct PoolState {
    /// All active worker handles
    workers: Vec<Arc<PoolWorkerHandle>>,
    /// Indices of workers that are currently idle (have capacity)
    idle_workers: VecDeque<usize>,
    /// Map of UDF name -> set of worker indices that have it cached
    udf_to_workers: HashMap<Arc<str>, HashSet<usize>>,
    /// Total number of workers ever created (for indexing)
    total_created: usize,
}

#[cfg(feature = "python")]
impl Default for PoolState {
    fn default() -> Self {
        Self {
            workers: Vec::new(),
            idle_workers: VecDeque::new(),
            udf_to_workers: HashMap::new(),
            total_created: 0,
        }
    }
}

/// Manager for the global process pool
pub(crate) struct ProcessPoolManager {
    /// Maximum number of workers allowed
    max_workers: AtomicUsize,
    /// Current number of active workers
    active_workers: AtomicUsize,
    /// Notification for when a worker becomes available
    #[cfg(feature = "python")]
    notify: Notify,
    /// Pool state (workers, availability, UDF tracking)
    #[cfg(feature = "python")]
    state: Mutex<PoolState>,
    /// Compute runtime for spawning workers
    #[cfg(feature = "python")]
    runtime: RuntimeRef,
}

impl Default for ProcessPoolManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessPoolManager {
    pub fn new() -> Self {
        Self {
            max_workers: AtomicUsize::new(get_default_pool_size()),
            active_workers: AtomicUsize::new(0),
            #[cfg(feature = "python")]
            notify: Notify::new(),
            #[cfg(feature = "python")]
            state: Mutex::new(PoolState::default()),
            #[cfg(feature = "python")]
            runtime: get_compute_runtime(),
        }
    }

    /// Get the current maximum pool size
    pub fn max_workers(&self) -> usize {
        self.max_workers.load(Ordering::SeqCst)
    }

    /// Get the current number of active workers
    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::SeqCst)
    }

    /// Get pool statistics for testing/debugging
    /// Returns: (max_workers, active_workers, total_workers_ever_created)
    #[cfg(feature = "python")]
    pub fn get_stats(&self) -> (usize, usize, usize) {
        let state = self.state.lock().unwrap();
        (
            self.max_workers(),
            self.active_workers(),
            state.total_created,
        )
    }

    #[cfg(not(feature = "python"))]
    pub fn get_stats(&self) -> (usize, usize, usize) {
        (self.max_workers(), self.active_workers(), 0)
    }

    /// Request that the pool has at least `min_workers` capacity.
    /// This is used when a UDF has a `max_concurrency` that exceeds the default pool size.
    pub fn request_capacity(&self, min_workers: usize) {
        let current_max = self.max_workers.load(Ordering::SeqCst);
        if min_workers > current_max {
            log::info!(
                "Increasing UDF process pool capacity from {} to {} workers",
                current_max,
                min_workers
            );
            self.max_workers.store(min_workers, Ordering::SeqCst);
        }
    }

    /// Submit a task for execution and return the result.
    /// This will acquire a worker (spawning if necessary), execute the task,
    /// and release the worker back to the pool.
    #[cfg(feature = "python")]
    pub async fn submit_task(
        &self,
        task: &UdfTask,
        input: RecordBatch,
    ) -> DaftResult<(Series, OperatorMetrics)> {
        // Acquire a worker handle (preferring one that already has this UDF cached)
        let (worker_handle, worker_idx) = self.acquire_worker_handle(&task.udf_name).await?;

        // Submit task to worker (no locks held during execution)
        let result = worker_handle.submit(task, input).await;

        // Update UDF -> worker mapping if task succeeded
        if result.is_ok() {
            let mut state = self.state.lock().unwrap();
            state
                .udf_to_workers
                .entry(task.udf_name.clone())
                .or_default()
                .insert(worker_idx);
        }

        // Release worker back to pool
        self.release_worker_handle(worker_handle, worker_idx);

        result
    }

    /// Acquire a worker handle for executing a task.
    /// Prefers workers that already have the UDF cached.
    /// Will spawn a new worker if none are available and under the limit.
    /// Returns (worker_handle, worker_idx)
    #[cfg(feature = "python")]
    async fn acquire_worker_handle(&self, udf_name: &str) -> DaftResult<(Arc<PoolWorkerHandle>, usize)> {
        loop {
            {
                let mut state = self.state.lock().unwrap();

                // First, try to find an idle worker that already has this UDF cached
                let cached_worker_indices: Vec<usize> = state
                    .udf_to_workers
                    .get(udf_name)
                    .map(|indices| indices.iter().copied().collect())
                    .unwrap_or_default();

                for idx in cached_worker_indices {
                    if let Some(pos) = state.idle_workers.iter().position(|&i| i == idx) {
                        state.idle_workers.remove(pos);
                        if idx < state.workers.len() {
                            return Ok((state.workers[idx].clone(), idx));
                        }
                    }
                }

                // Try to get any idle worker
                if let Some(idx) = state.idle_workers.pop_front() {
                    if idx < state.workers.len() {
                        return Ok((state.workers[idx].clone(), idx));
                    }
                }

                // Try to spawn a new worker if under the limit
                let active = self.active_workers.load(Ordering::SeqCst);
                let max = self.max_workers.load(Ordering::SeqCst);
                if active < max {
                    let worker = Arc::new(PoolWorkerHandle::new(state.total_created, self.runtime.clone())?);
                    let worker_idx = state.total_created;
                    state.total_created += 1;
                    state.workers.push(worker.clone());
                    state.idle_workers.push_back(worker_idx);
                    self.active_workers.fetch_add(1, Ordering::SeqCst);
                    return Ok((worker, worker_idx));
                }
            }

            // No workers available, wait for one to be released
            self.notify.notified().await;
        }
    }

    /// Release a worker handle back to the idle pool
    #[cfg(feature = "python")]
    fn release_worker_handle(&self, _worker_handle: Arc<PoolWorkerHandle>, worker_idx: usize) {
        let mut state = self.state.lock().unwrap();
        // Verify the worker index is valid and add back to idle pool
        if worker_idx < state.workers.len() && !state.idle_workers.contains(&worker_idx) {
            state.idle_workers.push_back(worker_idx);
        }
        drop(state);
        self.notify.notify_waiters();
    }

    /// Signal that a UDF is complete and should be removed from all worker caches.
    /// Workers with empty caches after this will self-terminate.
    #[cfg(feature = "python")]
    pub fn teardown_udf(&self, udf_name: &str) {
        let mut state = self.state.lock().unwrap();

        // Get the set of workers that have this UDF cached
        let worker_indices = match state.udf_to_workers.remove(udf_name) {
            Some(indices) => indices,
            None => return, // No workers have this UDF
        };

        // Tell each worker to teardown this UDF
        let mut workers_to_remove = Vec::new();
        let workers_to_teardown: Vec<_> = worker_indices
            .iter()
            .filter_map(|&idx| {
                if idx < state.workers.len() {
                    Some((idx, state.workers[idx].clone()))
                } else {
                    None
                }
            })
            .collect();
        drop(state);

        // Teardown UDFs on workers (async, outside the lock)
        for (idx, worker) in workers_to_teardown {
            let udf_name = udf_name.to_string();
            let worker_clone = worker.clone();
            self.runtime.spawn(async move {
                if let Ok(true) = worker_clone.teardown_udf(&udf_name).await {
                    // Worker exited, will be handled below
                }
            });
            
            // Check if worker should be removed (cache empty)
            if worker.active_udfs_count() == 0 {
                workers_to_remove.push(idx);
            }
        }

        // Remove workers that exited
        let mut state = self.state.lock().unwrap();

        // Remove workers that exited
        for idx in workers_to_remove {
            state.idle_workers.retain(|&i| i != idx);
            for workers in state.udf_to_workers.values_mut() {
                workers.remove(&idx);
            }
            self.active_workers.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Shutdown all workers in the pool
    #[cfg(feature = "python")]
    pub fn shutdown(&self) {
        let mut state = self.state.lock().unwrap();
        for worker in state.workers.drain(..) {
            // Clone sender before trying to unwrap
            let sender = worker.sender.clone();
            // Try to unwrap the Arc to get ownership for shutdown
            if let Ok(worker) = Arc::try_unwrap(worker) {
                self.runtime.spawn(async move {
                    let _ = worker.shutdown().await;
                });
            } else {
                // Multiple references exist, just send shutdown message
                let _ = sender.send(WorkerMessage::Shutdown);
            }
        }
        state.idle_workers.clear();
        state.udf_to_workers.clear();
        self.active_workers.store(0, Ordering::SeqCst);
    }

    // Non-python stubs for compilation
    #[cfg(not(feature = "python"))]
    pub async fn submit_task(
        &self,
        _task: &UdfTask,
        _input: daft_recordbatch::RecordBatch,
    ) -> DaftResult<(daft_core::series::Series, OperatorMetrics)> {
        panic!("Cannot submit UDF task without Python feature");
    }

    #[cfg(not(feature = "python"))]
    pub fn teardown_udf(&self, _udf_name: &str) {
        panic!("Cannot teardown UDF without Python feature");
    }

    #[cfg(not(feature = "python"))]
    pub fn shutdown(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_pool_size() {
        let pool = ProcessPoolManager::new();
        assert!(pool.max_workers() > 0);
    }

    #[test]
    fn test_request_capacity() {
        let pool = ProcessPoolManager::new();
        let initial = pool.max_workers();

        // Request higher capacity
        pool.request_capacity(initial + 10);
        assert_eq!(pool.max_workers(), initial + 10);

        // Request lower capacity (should not decrease)
        pool.request_capacity(initial);
        assert_eq!(pool.max_workers(), initial + 10);
    }
}
