//! Global process pool for UDF execution.
//!
//! This module provides a shared pool of Python worker processes that can execute
//! multiple UDFs. Workers are lazily spawned and cache UDF state for efficient reuse.

use std::{
    collections::{HashMap, HashSet},
    sync::{Condvar, Mutex},
};

use common_error::DaftResult;
use daft_core::series::Series;
use daft_dsl::{ExprRef, functions::python::UDFName, operator_metrics::OperatorMetrics};
use daft_recordbatch::RecordBatch;
#[cfg(feature = "python")]
use pyo3::{Py, PyAny, Python, prelude::*};

use crate::STDOUT;

/// Rust-side handle to a pool worker
#[cfg(feature = "python")]
struct UdfWorkerHandle {
    /// Python worker handle
    py_handle: Py<PyAny>,
    /// Set of active UDF names in this worker
    active_udfs: HashSet<UDFName>,
    /// Worker index for logging
    index: usize,
}

#[cfg(feature = "python")]
impl std::fmt::Debug for UdfWorkerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdfWorkerHandle")
            .field("active_udfs", &self.active_udfs)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "python")]
impl UdfWorkerHandle {
    /// Create a new UDF worker handle
    fn new(index: usize) -> DaftResult<Self> {
        // Create Python worker handle
        let py_handle = Python::attach(|py| {
            Ok::<Py<PyAny>, PyErr>(
                py.import(pyo3::intern!(py, "daft.execution.udf"))?
                    .getattr(pyo3::intern!(py, "UdfWorkerHandle"))?
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

    fn is_alive(&self) -> bool {
        Python::attach(|py| -> bool {
            self.py_handle
                .bind(py)
                .call_method0(pyo3::intern!(py, "is_alive"))
                .and_then(|r| r.extract::<bool>())
                .unwrap_or(false)
        })
    }

    /// Submit a UDF task for execution
    fn submit(
        &mut self,
        udf_name: &UDFName,
        expr: &ExprRef,
        input: RecordBatch,
    ) -> DaftResult<(Series, OperatorMetrics)> {
        use common_metrics::python::PyOperatorMetrics;
        use daft_dsl::python::PyExpr;
        use daft_recordbatch::python::PyRecordBatch;

        // Check if this is a new UDF (needs initialization)
        let is_new_udf = self.active_udfs.insert(udf_name.clone());

        let result = Python::attach(|py| -> DaftResult<_> {
            // Initialize the UDF if it's new
            if is_new_udf {
                let py_expr = PyExpr::from(expr.clone());
                self.py_handle
                    .bind(py)
                    .call_method1(pyo3::intern!(py, "init_udf"), (udf_name.as_ref(), py_expr))?;
            }

            // Execute the UDF
            let (py_result, py_stdout_lines, py_metrics) = self
                .py_handle
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "eval_input"),
                    (udf_name.as_ref(), PyRecordBatch::from(input.clone())),
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
            let label = format!("[`{}` Worker #{}]", udf_name, self.index);
            for line in stdout_lines {
                STDOUT.print(&label, line);
            }
        }

        let (result_batch, _stdout, metrics) = result?;
        debug_assert!(
            result_batch.num_columns() == 1,
            "UDF should return a single column"
        );
        Ok((result_batch.get_column(0).clone(), metrics))
    }

    /// Clean up multiple UDFs from this UDF worker in batch
    fn cleanup_udfs(&mut self, finished_udfs: &HashSet<UDFName>) {
        // Filter to only UDFs that are actually active in this worker
        let udfs_to_cleanup = finished_udfs
            .iter()
            .filter(|finished_udf_name| self.active_udfs.contains(*finished_udf_name))
            .map(|finished_udf_name| finished_udf_name.as_ref())
            .collect::<Vec<_>>();
        if udfs_to_cleanup.is_empty() {
            return;
        }

        // Clean up the UDFs
        let _ = Python::attach(|py| -> PyResult<()> {
            self.py_handle
                .bind(py)
                .call_method1(pyo3::intern!(py, "cleanup_udfs"), (&udfs_to_cleanup,))?;
            Ok(())
        });
        // Remove all cleaned up UDFs from active set
        for udf_name in udfs_to_cleanup {
            self.active_udfs.remove(udf_name);
        }
    }

    /// Get the number of active UDFs in this worker
    fn active_udfs_count(&self) -> usize {
        self.active_udfs.len()
    }

    /// Check if a UDF is active in this worker
    fn has_udf(&self, udf_name: &str) -> bool {
        self.active_udfs.contains(udf_name)
    }

    /// Shutdown the worker
    fn shutdown(&self) {
        // Shutdown the Python worker
        let _ = Python::attach(|py| -> PyResult<()> {
            self.py_handle
                .bind(py)
                .call_method0(pyo3::intern!(py, "shutdown"))?;
            Ok(())
        });
    }
}

#[cfg(feature = "python")]
impl Drop for UdfWorkerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

type WorkerIndex = usize;

/// State for managing UDF workers in the pool
#[cfg(feature = "python")]
#[derive(Default, Debug)]
struct PoolState {
    /// Workers that are available for use
    available_workers: HashMap<WorkerIndex, UdfWorkerHandle>,
    /// Number of workers currently in use (not in available_workers)
    workers_in_use: usize,
    /// Total number of workers ever created (for indexing)
    total_created: usize,
    /// UDFs that are currently active in the pool, and how many UdfHandles exist for each UDF
    active_udf_ref_counts: HashMap<UDFName, usize>,
    /// UDFs that have been completed and can be cleaned up
    completed_udfs: HashSet<UDFName>,
}

/// Manager for the global process pool
#[cfg(feature = "python")]
#[derive(Debug)]
pub(super) struct ProcessPoolManager {
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
    /// Returns: (active_workers, total_workers_ever_created)
    pub fn get_stats(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        let active_workers = state.available_workers.len() + state.workers_in_use;
        (active_workers, state.total_created)
    }

    pub fn reset_total_created(&self) {
        let mut state = self.state.lock().unwrap();
        state.total_created = 0;
    }

    /// Submit a task for execution and return the result.
    /// This will acquire a worker (spawning if necessary), execute the task,
    /// and release the worker back to the pool.
    pub fn submit_task(
        &self,
        udf_name: &UDFName,
        expr: &ExprRef,
        max_concurrency: usize,
        input: RecordBatch,
    ) -> DaftResult<(Series, OperatorMetrics)> {
        // Acquire a worker handle (preferring one that already has this UDF cached)
        let mut worker_handle = self.acquire_worker_handle(udf_name, max_concurrency)?;

        // Submit task to worker (no locks held during execution for parallelism)
        let result = worker_handle.submit(udf_name, expr, input);

        // Put the worker back in the pool (or discard if dead)
        self.release_worker_handle(worker_handle);
        result
    }

    /// Acquire a worker handle for executing a task.
    /// Prefers workers that already have the UDF cached.
    /// Will spawn a new worker if none are available and under the limit.
    fn acquire_worker_handle(
        &self,
        udf_name: &UDFName,
        udf_max_concurrency: usize,
    ) -> DaftResult<UdfWorkerHandle> {
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

            // Try to spawn a new worker if under the limit
            let total_workers = state.available_workers.len() + state.workers_in_use;
            if total_workers < udf_max_concurrency {
                let worker_idx = state.total_created;
                state.total_created += 1;
                state.workers_in_use += 1;
                let worker_handle = UdfWorkerHandle::new(worker_idx)?;
                return Ok(worker_handle);
            }

            // No workers available, wait for one to be released
            state = self.condvar.wait(state).unwrap();
        }
    }

    /// Release a worker handle back to the available pool
    fn release_worker_handle(&self, worker_handle: UdfWorkerHandle) {
        // Check if worker is still alive
        let is_alive = worker_handle.is_alive();

        let mut state = self.state.lock().unwrap();
        // Decrement in-use count
        state.workers_in_use -= 1;

        // If worker is still alive, add it back to the available workers
        if is_alive {
            state
                .available_workers
                .insert(worker_handle.index, worker_handle);
        }

        // Notify a waiting thread to wake up and try to acquire a worker
        self.condvar.notify_one();
    }

    /// Register a UDF handle (increment reference count)
    /// Should be called when a UdfHandle is created
    pub fn register_udf_handle(&self, udf_name: UDFName) {
        let mut state = self.state.lock().unwrap();
        *state.active_udf_ref_counts.entry(udf_name).or_insert(0) += 1;
    }

    /// Unregister a UDF handle (decrement reference count and cleanup if zero)
    /// Should be called when a UdfHandle is dropped
    pub fn unregister_udf_handle(&self, udf_name: &UDFName) {
        let mut state = self.state.lock().unwrap();

        // Decrement reference count, mark as completed if zero
        if let Some(count) = state.active_udf_ref_counts.get_mut(udf_name) {
            *count -= 1;
            if *count == 0 {
                state.active_udf_ref_counts.remove(udf_name);
                state.completed_udfs.insert(udf_name.clone());
            }
        }

        // Cleanup completed UDFs from available workers, return workers that still have active UDFs
        let mut workers_to_return = Vec::new();
        for mut worker in std::mem::take(&mut state.available_workers).into_values() {
            worker.cleanup_udfs(&state.completed_udfs);
            if worker.active_udfs_count() > 0 {
                workers_to_return.push(worker);
            }
        }

        // Return workers that still have active UDFs
        state.available_workers = workers_to_return
            .into_iter()
            .map(|worker| (worker.index, worker))
            .collect();
    }
}
