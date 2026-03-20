use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, worker::RaySwordfishWorker};
use crate::scheduling::{
    scheduler::WorkerSnapshot,
    task::{SwordfishTask, TaskContext, TaskResourceRequest},
    worker::{Worker, WorkerId, WorkerManager},
};

const REFRESH_INTERVAL_SECS: Duration = Duration::from_secs(5);

struct RayWorkerManagerState {
    ray_workers: HashMap<WorkerId, RaySwordfishWorker>,
    last_refresh: Option<Instant>,
    max_resources_requested: ResourceRequest,
}

impl RayWorkerManagerState {
    fn refresh_workers(&mut self) -> DaftResult<()> {
        let should_refresh = match self.last_refresh {
            None => true,
            Some(last_time) => last_time.elapsed() > REFRESH_INTERVAL_SECS,
        };

        if !should_refresh {
            return Ok(());
        }

        let ray_workers = Python::attach(|py| {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;

            let existing_worker_ids = self
                .ray_workers
                .keys()
                .map(|id| id.as_ref().to_string())
                .collect::<Vec<_>>();

            let ray_workers = flotilla_module
                .call_method1(
                    pyo3::intern!(py, "start_ray_workers"),
                    (existing_worker_ids,),
                )?
                .extract::<Vec<RaySwordfishWorker>>()?;

            DaftResult::Ok(ray_workers)
        })?;

        for worker in ray_workers {
            self.ray_workers.insert(worker.id().clone(), worker);
        }
        self.last_refresh = Some(Instant::now());
        DaftResult::Ok(())
    }
}

// Wrapper around the RaySwordfishWorkerManager class in the distributed_swordfish module.
pub(crate) struct RayWorkerManager {
    state: Arc<Mutex<RayWorkerManagerState>>,
}

impl RayWorkerManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(RayWorkerManagerState {
                ray_workers: HashMap::new(),
                last_refresh: None,
                max_resources_requested: ResourceRequest::default(),
            })),
        }
    }
}

impl WorkerManager for RayWorkerManager {
    type Worker = RaySwordfishWorker;

    fn submit_tasks_to_workers(
        &self,
        tasks_per_worker: HashMap<WorkerId, Vec<SwordfishTask>>,
    ) -> DaftResult<Vec<RayTaskResultHandle>> {
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        let mut task_result_handles =
            Vec::with_capacity(tasks_per_worker.values().map(|v| v.len()).sum());

        Python::attach(|py| {
            for (worker_id, tasks) in tasks_per_worker {
                let handles = state
                    .ray_workers
                    .get_mut(&worker_id)
                    .ok_or_else(|| {
                        DaftError::ValueError(format!(
                            "Worker {worker_id} not found in RayWorkerManager when submitting tasks"
                        ))
                    })?
                    .submit_tasks(tasks, py)?;
                task_result_handles.extend(handles);
            }
            DaftResult::Ok(())
        })?;
        DaftResult::Ok(task_result_handles)
    }

    fn worker_snapshots(&self) -> DaftResult<Vec<WorkerSnapshot>> {
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");

        // Refresh workers if needed (internally rate-limited)
        state.refresh_workers()?;

        Ok(state
            .ray_workers
            .values()
            .map(WorkerSnapshot::from)
            .collect::<Vec<_>>())
    }

    fn mark_task_finished(&self, task_context: TaskContext, worker_id: WorkerId) {
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        if let Some(worker) = state.ray_workers.get_mut(&worker_id) {
            worker.mark_task_finished(&task_context);
        }
    }

    fn mark_worker_died(&self, worker_id: WorkerId) {
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        state.ray_workers.remove(&worker_id);
    }

    fn shutdown(&self) -> DaftResult<()> {
        let state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        Python::attach(|py| {
            for worker in state.ray_workers.values() {
                worker.shutdown(py);
            }
        });
        Ok(())
    }

    fn try_autoscale(&self, bundles: Vec<TaskResourceRequest>) -> DaftResult<()> {
        let (requested_num_cpus, requested_num_gpus, requested_memory_bytes) =
            bundles.iter().fold((0.0, 0.0, 0), |acc, bundle| {
                (
                    acc.0 + bundle.resource_request.num_cpus().unwrap_or(0.0),
                    acc.1 + bundle.resource_request.num_gpus().unwrap_or(0.0),
                    acc.2 + bundle.resource_request.memory_bytes().unwrap_or(0),
                )
            });

        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");

        let (available_num_cpus, available_num_gpus, available_memory_bytes) = state
            .ray_workers
            .values()
            .fold((0.0, 0.0, 0isize), |acc, worker| {
                (
                    acc.0 + worker.total_num_cpus() - worker.active_num_cpus(),
                    acc.1 + worker.total_num_gpus() - worker.active_num_gpus(),
                    acc.2 + worker.total_memory_bytes() as isize
                        - worker
                            .active_task_details()
                            .values()
                            .map(|d| d.memory_bytes() as isize)
                            .sum::<isize>(),
                )
            });

        let resource_request_greater_than_available_capacity = requested_num_cpus
            > available_num_cpus
            || requested_num_gpus > available_num_gpus
            || requested_memory_bytes as isize > available_memory_bytes;

        let resource_request_greater_than_max_requested = requested_num_cpus
            > state.max_resources_requested.num_cpus().unwrap_or(0.0)
            || requested_num_gpus > state.max_resources_requested.num_gpus().unwrap_or(0.0)
            || requested_memory_bytes > state.max_resources_requested.memory_bytes().unwrap_or(0);

        tracing::debug!(
            requested_num_cpus,
            requested_num_gpus,
            requested_memory_bytes,
            available_num_cpus,
            available_num_gpus,
            available_memory_bytes,
            watermark_cpus = state.max_resources_requested.num_cpus().unwrap_or(0.0),
            watermark_gpus = state.max_resources_requested.num_gpus().unwrap_or(0.0),
            watermark_memory = state.max_resources_requested.memory_bytes().unwrap_or(0),
            exceeds_available = resource_request_greater_than_available_capacity,
            exceeds_watermark = resource_request_greater_than_max_requested,
            "Autoscaling decision"
        );

        // Only autoscale if pending demand exceeds available capacity AND this is a new
        // all-time peak. The watermark deduplicates requests so we don't spam Ray with
        // repeated request_resources calls for the same demand level.
        // TODO: Reset the watermark when the cluster shrinks (requires tracking downscaling).
        if resource_request_greater_than_available_capacity
            && resource_request_greater_than_max_requested
        {
            state.max_resources_requested = ResourceRequest::try_new_internal(
                Some(requested_num_cpus),
                Some(requested_num_gpus),
                Some(requested_memory_bytes),
            )?;
            let python_bundles = bundles
                .iter()
                .map(|bundle| {
                    let mut dict = HashMap::new();
                    dict.insert("CPU", bundle.num_cpus().ceil() as i64);
                    let gpu = bundle.num_gpus().ceil() as i64;
                    if gpu > 0 {
                        dict.insert("GPU", gpu);
                    }
                    let memory = bundle.memory_bytes() as i64;
                    if memory > 0 {
                        dict.insert("memory", memory);
                    }
                    dict
                })
                .collect::<Vec<_>>();

            Python::attach(|py| -> DaftResult<()> {
                let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
                flotilla_module
                    .call_method1(pyo3::intern!(py, "try_autoscale"), (python_bundles,))?;
                Ok(())
            })?;
        }
        Ok(())
    }
}
