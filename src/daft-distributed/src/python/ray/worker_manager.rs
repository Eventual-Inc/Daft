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
    pending_release_blacklist: HashMap<WorkerId, Instant>,
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
        // Exclude pending-release workers for a grace TTL to prevent immediate respawn
        let ttl_secs: u64 = std::env::var("DAFT_AUTOSCALING_PENDING_RELEASE_EXCLUDE_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(120);
        self.pending_release_blacklist
            .retain(|_, ts| ts.elapsed() < Duration::from_secs(ttl_secs));

        let ray_workers = Python::attach(|py| {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;

            let mut existing_worker_ids = self
                .ray_workers
                .keys()
                .map(|id| id.as_ref().to_string())
                .collect::<Vec<_>>();
            existing_worker_ids.extend(
                self.pending_release_blacklist
                    .keys()
                    .map(|id| id.as_ref().to_string()),
            );

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
                pending_release_blacklist: HashMap::new(),
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

        // If no desired bundles, clear outstanding autoscaler requests instead of scale-up.
        if bundles.is_empty()
            || (requested_num_cpus <= 0.0
                && requested_num_gpus <= 0.0
                && requested_memory_bytes == 0)
        {
            Python::attach(|py| -> DaftResult<()> {
                let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
                flotilla_module.call_method0(pyo3::intern!(py, "clear_autoscaling_requests"))?;
                Ok(())
            })?;
            state.max_resources_requested = ResourceRequest::default();
            return Ok(());
        }

        let (cluster_num_cpus, cluster_num_gpus, cluster_memory_bytes) = state
            .ray_workers
            .values()
            .fold((0.0, 0.0, 0), |acc, worker| {
                (
                    acc.0 + worker.total_num_cpus(),
                    acc.1 + worker.total_num_gpus(),
                    acc.2 + worker.total_memory_bytes(),
                )
            });

        let resource_request_greater_than_current_capacity = requested_num_cpus > cluster_num_cpus
            || requested_num_gpus > cluster_num_gpus
            || requested_memory_bytes > cluster_memory_bytes;

        let resource_request_greater_than_max_requested = requested_num_cpus
            > state.max_resources_requested.num_cpus().unwrap_or(0.0)
            || requested_num_gpus > state.max_resources_requested.num_gpus().unwrap_or(0.0)
            || requested_memory_bytes > state.max_resources_requested.memory_bytes().unwrap_or(0);

        let cluster_is_zero_capacity =
            cluster_num_cpus <= 0.0 && cluster_num_gpus <= 0.0 && cluster_memory_bytes == 0;
        let should_bootstrap = cluster_is_zero_capacity
            && (requested_num_cpus > 0.0 || requested_num_gpus > 0.0 || requested_memory_bytes > 0);

        // Only autoscale if we need more capacity AND this is greater than we've seen before
        if (resource_request_greater_than_current_capacity
            && resource_request_greater_than_max_requested)
            || should_bootstrap
        {
            // On scale-up demand, allow previously blacklisted workers to be reused immediately.
            state.pending_release_blacklist.clear();
            state.last_refresh = None;
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
                    dict.insert("GPU", bundle.num_gpus().ceil() as i64);
                    dict.insert("memory", bundle.memory_bytes() as i64);
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

    fn retire_idle_ray_workers(
        &self,
        max_to_retire: usize,
        force_all_when_cluster_idle: bool,
    ) -> DaftResult<usize> {
        if force_all_when_cluster_idle {
            Python::attach(|py| -> DaftResult<()> {
                let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
                flotilla_module.call_method0(pyo3::intern!(py, "clear_autoscaling_requests"))?;
                Ok(())
            })?;
        }

        if max_to_retire == 0 {
            return Ok(0);
        }

        let idle_secs_threshold: Option<u64> = if force_all_when_cluster_idle {
            None
        } else {
            Some(
                std::env::var("DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(60),
            )
        };

        let now = Instant::now();

        // Determine the Ray head node id so we can avoid retiring its worker.
        // Head node cannot be released by Ray autoscaler, and we should always
        // keep its SwordfishActor alive even if idle.
        let head_node_id: Option<String> = Python::attach(|py| {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            let head_id_obj =
                flotilla_module.call_method0(pyo3::intern!(py, "get_head_node_id"))?;
            let head_id = head_id_obj.extract::<Option<String>>()?;
            DaftResult::Ok(head_id)
        })?;

        let (workers_to_release, survivors_after, blacklisted_after) = {
            let mut state = self
                .state
                .lock()
                .expect("Failed to lock RayWorkerManagerState");

            let mut candidates: Vec<(WorkerId, Duration)> = state
                .ray_workers
                .iter()
                .filter_map(|(wid, w)| {
                    // Skip the head node entirely from retirement consideration.
                    if let Some(ref head_id) = head_node_id
                        && wid.as_ref() == head_id
                    {
                        return None;
                    }
                    if w.is_idle() {
                        let idle_for = w.idle_duration(now);
                        if let Some(threshold) = idle_secs_threshold {
                            if idle_for.as_secs() >= threshold {
                                Some((wid.clone(), idle_for))
                            } else {
                                None
                            }
                        } else {
                            Some((wid.clone(), idle_for))
                        }
                    } else {
                        None
                    }
                })
                .collect();

            candidates.sort_by_key(|(_, d)| std::cmp::Reverse(d.as_secs()));

            let selected: Vec<(WorkerId, Duration)> =
                candidates.into_iter().take(max_to_retire).collect();

            let mut workers_to_release = Vec::with_capacity(selected.len());
            for (wid, _idle_for) in selected {
                if let Some(worker) = state.ray_workers.remove(&wid) {
                    state
                        .pending_release_blacklist
                        .insert(wid.clone(), Instant::now());
                    workers_to_release.push(worker);
                }
            }

            let survivors_after = state.ray_workers.len();
            let blacklisted_after = state.pending_release_blacklist.len();

            state.max_resources_requested = ResourceRequest::default();
            state.last_refresh = None;

            (workers_to_release, survivors_after, blacklisted_after)
        };

        if workers_to_release.is_empty() {
            return Ok(0);
        }

        tracing::info!(
            target: "ray_worker_manager",
            "Preparing to release {} workers",
            workers_to_release.len()
        );

        let mut released = 0usize;
        Python::attach(|py| -> DaftResult<()> {
            for mut worker in workers_to_release {
                worker.release(py);
                released += 1;
            }
            Ok(())
        })?;

        Python::attach(|py| -> DaftResult<()> {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            flotilla_module.call_method0(pyo3::intern!(py, "clear_autoscaling_requests"))?;
            Ok(())
        })?;

        tracing::info!(
            target: "ray_worker_manager",
            released,
            survivors = survivors_after,
            blacklisted = blacklisted_after,
            "Idle cleanup completed"
        );

        Ok(released)
    }
}
