//! Global process pool for UDF execution.
//!
//! This module provides a shared pool of Python worker processes that can execute
//! multiple UDFs. Workers are lazily spawned and cache UDF state for efficient reuse.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Condvar, Mutex, OnceLock},
};

use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
use daft_core::series::Series;
use daft_dsl::{ExprRef, operator_metrics::OperatorMetrics};
use daft_recordbatch::RecordBatch;
#[cfg(feature = "python")]
use pyo3::{Py, PyAny, Python, prelude::*};

use crate::STDOUT;

/// Global process pool instance
#[cfg(feature = "python")]
pub(crate) static PROCESS_POOL: OnceLock<Arc<ProcessPoolManager>> = OnceLock::new();

/// Get or initialize the global process pool
#[cfg(feature = "python")]
pub(crate) fn get_or_init_process_pool() -> &'static Arc<ProcessPoolManager> {
    PROCESS_POOL.get_or_init(|| Arc::new(ProcessPoolManager::new()))
}

/// Get process pool statistics (exported to Python)
#[cfg(feature = "python")]
#[pyo3::pyfunction]
#[pyo3(name = "_get_process_pool_stats")]
pub fn _get_process_pool_stats() -> (usize, usize, usize) {
    get_or_init_process_pool().get_stats()
}

/// Environment variable for configuring pool size
const POOL_SIZE_ENV_VAR: &str = "DAFT_UDF_POOL_SIZE";

fn get_default_pool_size() -> usize {
    if let Ok(val) = std::env::var(POOL_SIZE_ENV_VAR)
        && let Ok(size) = val.parse::<usize>()
    {
        return size.max(1);
    }
    get_compute_pool_num_threads()
}

/// A task to be executed by a pool worker
#[derive(Clone)]
pub(crate) struct UdfTask {
    /// Unique identifier for the UDF (typically the full function name)
    pub udf_name: Arc<str>,
    /// The expression for this UDF (will be pickled in Python)
    pub expr: ExprRef,
    /// The maximum concurrency for this UDF
    pub max_concurrency: usize,
}

/// Rust-side handle to a pool worker
#[cfg(feature = "python")]
pub(crate) struct PoolWorkerHandle {
    /// Python worker handle
    py_handle: Py<PyAny>,
    /// Set of active UDF names in this worker
    active_udfs: HashSet<Arc<str>>,
    /// Worker index for logging
    index: usize,
}

#[cfg(feature = "python")]
impl PoolWorkerHandle {
    /// Create a new pool worker handle
    fn new(index: usize) -> DaftResult<Self> {
        // Create Python worker handle
        let py_handle = Python::attach(|py| {
            Ok::<Py<PyAny>, PyErr>(
                py.import(pyo3::intern!(py, "daft.execution.udf"))?
                    .getattr(pyo3::intern!(py, "PoolWorkerHandle"))?
                    .call0()?
                    .unbind(),
            )
        })?;

        let active_udfs = HashSet::new();

        Ok(Self {
            py_handle,
            active_udfs,
            index,
        })
    }

    /// Submit a UDF task for execution
    #[cfg(feature = "python")]
    pub fn submit(
        &mut self,
        task: &UdfTask,
        input: RecordBatch,
    ) -> DaftResult<(Series, OperatorMetrics)> {
        use common_metrics::python::PyOperatorMetrics;
        use daft_recordbatch::python::PyRecordBatch;

        // Check if this is a new UDF (needs initialization)
        let is_new_udf = !self.active_udfs.contains(&task.udf_name);

        // Initialize UDF if it's new (no locks held during Python call for parallelism)
        if is_new_udf {
            Python::attach(|py| -> DaftResult<()> {
                use daft_dsl::python::PyExpr;
                // Convert ExprRef to PyExpr and let Python do the pickling
                let py_expr = PyExpr::from(task.expr.clone());
                self.py_handle.bind(py).call_method1(
                    pyo3::intern!(py, "init_udf"),
                    (task.udf_name.as_ref(), py_expr),
                )?;
                Ok(())
            })?;
            // Track UDF as active after successful initialization
            self.active_udfs.insert(task.udf_name.clone());
        }

        // Execute the UDF (no locks held during Python call for parallelism)
        let result = Python::attach(|py| -> DaftResult<_> {
            let (py_result, py_stdout_lines, py_metrics) = self
                .py_handle
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "eval_input"),
                    (task.udf_name.as_ref(), PyRecordBatch::from(input.clone())),
                )?
                .extract::<(PyRecordBatch, Vec<String>, PyOperatorMetrics)>()?;

            Ok((
                RecordBatch::from(py_result),
                py_stdout_lines,
                py_metrics.inner,
            ))
        });

        // Print stdout lines if call succeeded
        if let Ok((_, stdout_lines, _)) = &result {
            let label = format!("[Pool Worker #{}]", self.index);
            for line in stdout_lines {
                STDOUT.print(&label, line);
            }
        }

        // If call failed and UDF was new, remove from active_udfs if Python didn't cache it
        // (But actually, Python caches it when expr_bytes is provided, so we should keep it)
        // Actually, if the call fails before Python can cache it, we should remove it
        // But we can't know that, so we'll keep it and let cleanup handle it

        let (result_batch, _stdout, metrics) = result?;
        debug_assert!(
            result_batch.num_columns() == 1,
            "UDF should return a single column"
        );
        Ok((result_batch.get_column(0).clone(), metrics))
    }

    /// Clean up a UDF from this worker
    #[cfg(feature = "python")]
    pub fn cleanup_udf(&mut self, udf_name: &str) {
        if !self.active_udfs.contains(udf_name) {
            return;
        }

        // Call cleanup on Python worker (no locks held during Python call)
        let _ = Python::attach(|py| -> PyResult<()> {
            self.py_handle
                .bind(py)
                .call_method1(pyo3::intern!(py, "cleanup_udf"), (udf_name,))?;
            Ok(())
        });

        // Remove from active UDFs after successful cleanup
        self.active_udfs.remove(udf_name);
    }

    /// Get the number of active UDFs in this worker
    #[cfg(feature = "python")]
    pub fn active_udfs_count(&self) -> usize {
        self.active_udfs.len()
    }

    /// Check if a UDF is active in this worker
    #[cfg(feature = "python")]
    pub fn has_udf(&self, udf_name: &str) -> bool {
        self.active_udfs.contains(udf_name)
    }

    /// Shutdown the worker
    #[cfg(feature = "python")]
    pub fn shutdown(&self) {
        // Shutdown the Python worker
        let _ = Python::attach(|py| -> PyResult<()> {
            self.py_handle
                .bind(py)
                .call_method0(pyo3::intern!(py, "shutdown"))?;
            Ok(())
        });
    }
}

type WorkerIndex = usize;

/// State for managing workers in the pool
#[cfg(feature = "python")]
struct PoolState {
    /// Workers that are available for use
    available_workers: HashMap<WorkerIndex, PoolWorkerHandle>,
    /// Number of workers currently in use (not in available_workers)
    workers_in_use: usize,
    /// Total number of workers ever created (for indexing)
    total_created: usize,
    /// Maximum concurrency requested across all UDFs
    max_concurrency: usize,
}

#[cfg(feature = "python")]
impl Default for PoolState {
    fn default() -> Self {
        Self {
            available_workers: HashMap::new(),
            workers_in_use: 0,
            total_created: 0,
            max_concurrency: get_default_pool_size(),
        }
    }
}

/// Manager for the global process pool
#[cfg(feature = "python")]
pub(crate) struct ProcessPoolManager {
    /// Pool state (workers, availability, UDF tracking) with condition variable
    state: Mutex<PoolState>,
    /// Condition variable for waiting on worker availability
    condvar: Condvar,
}

#[cfg(feature = "python")]
impl Default for ProcessPoolManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "python")]
impl ProcessPoolManager {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(PoolState::default()),

            condvar: Condvar::new(),
        }
    }

    /// Get pool statistics for testing/debugging
    /// Returns: (max_workers, active_workers, total_workers_ever_created)
    pub fn get_stats(&self) -> (usize, usize, usize) {
        let state = self.state.lock().unwrap();
        let active_workers = state.available_workers.len() + state.workers_in_use;
        (state.max_concurrency, active_workers, state.total_created)
    }

    /// Submit a task for execution and return the result.
    /// This will acquire a worker (spawning if necessary), execute the task,
    /// and release the worker back to the pool.
    pub fn submit_task(
        &self,
        task: &UdfTask,
        input: RecordBatch,
    ) -> DaftResult<(Series, OperatorMetrics)> {
        // Acquire a worker handle (preferring one that already has this UDF cached)
        let mut worker_handle = self.acquire_worker_handle(&task.udf_name, task.max_concurrency)?;

        // Submit task to worker (no locks held during execution for parallelism)
        let result = worker_handle.submit(task, input);

        // If the worker died during execution, remove it from the pool instead of releasing it
        if result.is_err() {
            // Check if worker is still alive
            let is_alive = Python::attach(|py| -> bool {
                worker_handle
                    .py_handle
                    .bind(py)
                    .call_method0(pyo3::intern!(py, "is_alive"))
                    .and_then(|r| r.extract::<bool>())
                    .unwrap_or(false)
            });
            if !is_alive {
                // Worker died, decrement in-use count (don't release it back)
                let mut state = self.state.lock().unwrap();
                state.workers_in_use -= 1;
                self.condvar.notify_one(); // Notify waiting threads
                return result;
            }
        }

        // Put the worker back in the pool and notify any waiting threads
        self.release_worker_handle(worker_handle);

        result
    }

    /// Acquire a worker handle for executing a task.
    /// Prefers workers that already have the UDF cached.
    /// Will spawn a new worker if none are available and under the limit.
    fn acquire_worker_handle(
        &self,
        udf_name: &str,
        udf_max_concurrency: usize,
    ) -> DaftResult<PoolWorkerHandle> {
        let mut state = self.state.lock().unwrap();
        loop {
            // Try to find a worker that already has the UDF cached
            if let Some(worker_idx) = state
                .available_workers
                .iter()
                .find(|(_, w)| w.has_udf(udf_name))
                .map(|(idx, _)| *idx)
            {
                let worker_handle = state.available_workers.remove(&worker_idx).unwrap();
                state.workers_in_use += 1;
                return Ok(worker_handle);
            }

            // Try to get any available worker
            if let Some((&worker_idx, _)) = state.available_workers.iter().next() {
                let worker_handle = state.available_workers.remove(&worker_idx).unwrap();
                state.workers_in_use += 1;
                return Ok(worker_handle);
            }

            // Update max_concurrency if needed
            if udf_max_concurrency > state.max_concurrency {
                state.max_concurrency = udf_max_concurrency;
            }

            // Try to spawn a new worker if under the limit
            let total_workers = state.available_workers.len() + state.workers_in_use;
            if total_workers < state.max_concurrency {
                let worker_idx = state.total_created;
                state.total_created += 1;
                state.workers_in_use += 1;
                let worker_handle = PoolWorkerHandle::new(worker_idx)?;
                return Ok(worker_handle);
            }

            // No workers available, wait for one to be released
            state = self.condvar.wait(state).unwrap();
        }
    }

    /// Release a worker handle back to the available pool
    fn release_worker_handle(&self, worker_handle: PoolWorkerHandle) {
        let mut state = self.state.lock().unwrap();
        let worker_idx = worker_handle.index;
        // Put worker back in available_workers and decrement in-use count
        state.available_workers.insert(worker_idx, worker_handle);
        state.workers_in_use -= 1;
        // Notify waiting threads that a worker is now available
        self.condvar.notify_one();
    }

    /// Signal that a UDF is complete and should be removed from all worker caches.
    /// Workers with empty caches after this will self-terminate.
    pub fn cleanup_udf(&self, udf_name: &str) {
        let mut state = self.state.lock().unwrap();

        let mut workers_to_remove = Vec::new();
        for worker in state.available_workers.values_mut() {
            worker.cleanup_udf(udf_name);
            if worker.active_udfs_count() == 0 {
                workers_to_remove.push(worker.index);
            }
        }

        for worker_idx in workers_to_remove {
            let worker = state.available_workers.remove(&worker_idx).unwrap();
            worker.shutdown();
        }
    }
}
