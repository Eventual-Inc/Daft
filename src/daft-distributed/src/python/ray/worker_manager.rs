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
const DEFAULT_AUTOSCALE_INTERVAL_SECS: u64 = 5;
// Environment variable Ray itself reads to configure its autoscaler reconciliation period.
// We read the same variable so our rate-limit matches Ray's actual cycle length.
const RAY_AUTOSCALER_UPDATE_INTERVAL_ENV: &str = "AUTOSCALER_UPDATE_INTERVAL_S";

struct RayWorkerManagerState {
    ray_workers: HashMap<WorkerId, RaySwordfishWorker>,
    last_refresh: Option<Instant>,
    max_resources_requested: ResourceRequest,
    pending_release_blacklist: HashMap<WorkerId, Instant>,
    last_autoscale_request_time: Option<Instant>,
    autoscale_interval_secs: Duration,
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

        // Exclude pending-release workers for a grace TTL to prevent immediate respawn.
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
                last_autoscale_request_time: None,
                autoscale_interval_secs: Duration::from_secs(
                    std::env::var(RAY_AUTOSCALER_UPDATE_INTERVAL_ENV)
                        .ok()
                        .and_then(|val| val.parse::<u64>().ok())
                        .unwrap_or(DEFAULT_AUTOSCALE_INTERVAL_SECS),
                ),
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

    /// Autoscale the Ray cluster by requesting resources from Ray's autoscaler.
    ///
    /// Constraints we operate under:
    /// - There is no reliable programmatic way for Daft to know the cluster's true autoscaling
    ///   ceiling ahead of time (for example, KubeRay `maxReplicas` or other external limits).
    /// - Daft can only observe currently registered Ray workers; it cannot directly account for
    ///   capacity that has already been requested but is still provisioning.
    /// - `ray.autoscaler.sdk.request_resources(bundles=...)` is **asynchronous** and each
    ///   call **replaces** the current demand (it is not additive).
    /// - Ray's autoscaler reconciliation loop processes the request every ~5 seconds by default
    ///   (configurable via `AUTOSCALER_UPDATE_INTERVAL_S`). Calls between cycles overwrite
    ///   each other — only the latest value at reconciliation time is processed.
    /// - If the requested bundles exceed the cluster's maximum capacity (e.g., KubeRay
    ///   `maxReplicas`), the autoscaler refuses to scale **at all** — not even partially.
    /// - We cannot detect whether the Ray autoscaler accepted or rejected the request, and
    ///   observing new workers is not a reliable signal for whether a request succeeded, since
    ///   node provisioning time varies (seconds to minutes depending on the environment).
    ///
    /// Algorithm: since we cannot detect failures and don't know the cluster's max capacity,
    /// we ramp up demand gradually. In each autoscaler cycle, we send one more bundle than the
    /// previous request (tracked via `max_resources_requested` as a high-water mark). The
    /// high-water mark is floored to current cluster resources so the very first cycle
    /// immediately requests scaling beyond current capacity.
    fn try_autoscale(&self, bundles: Vec<TaskResourceRequest>) -> DaftResult<()> {
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");

        // 1. Only attempt to grow the request once per Ray autoscaler reconciliation cycle.
        //    Sending more frequently would just overwrite the previous value before Ray processes it.
        if let Some(last_time) = state.last_autoscale_request_time
            && last_time.elapsed() < state.autoscale_interval_secs
        {
            return Ok(());
        }

        // 2. Floor the high-water mark to at least the current cluster's total resources.
        //    On cold start (high-water mark is 0), this lets us skip straight to requesting
        //    beyond current capacity on the very first cycle. When new workers join between
        //    cycles, this jumps the mark up so we don't waste cycles re-requesting resources
        //    the cluster already has.
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
        let high_water_mark_cpus = state
            .max_resources_requested
            .num_cpus()
            .unwrap_or(0.0)
            .max(cluster_num_cpus);
        let high_water_mark_gpus = state
            .max_resources_requested
            .num_gpus()
            .unwrap_or(0.0)
            .max(cluster_num_gpus);
        let high_water_mark_memory = state
            .max_resources_requested
            .memory_bytes()
            .unwrap_or(0)
            .max(cluster_memory_bytes);

        // 3. Accumulate bundles one at a time until the running total surpasses the
        //    high-water mark in any resource dimension (CPU, GPU, or memory). This ensures
        //    each cycle's request is exactly one bundle larger than the previous max —
        //    gradual enough to avoid exceeding an unknown cluster capacity limit.
        let mut cpu_sum = 0.0;
        let mut gpu_sum = 0.0;
        let mut memory_sum = 0;
        let mut surpassed = false;
        let mut selected_bundles = Vec::new();
        for bundle in &bundles {
            cpu_sum += bundle.resource_request.num_cpus().unwrap_or(0.0);
            gpu_sum += bundle.resource_request.num_gpus().unwrap_or(0.0);
            memory_sum += bundle.resource_request.memory_bytes().unwrap_or(0);
            selected_bundles.push(bundle);
            if cpu_sum > high_water_mark_cpus
                || gpu_sum > high_water_mark_gpus
                || memory_sum > high_water_mark_memory
            {
                surpassed = true;
                break;
            }
        }

        // 4. If we went through all pending bundles without surpassing the high-water mark,
        //    the remaining demand is smaller than what we previously requested. Skip this
        //    cycle — Ray still holds our previous (larger) request, so no downscale occurs.
        if !surpassed {
            return Ok(());
        }

        // 5. Send the selected bundles to Ray's autoscaler via request_resources().
        //    Strip zero-valued GPU/memory keys so Ray doesn't interpret them as a demand
        //    for zero-resource bundles on specialized nodes.
        let python_bundles = selected_bundles
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
            flotilla_module.call_method1(pyo3::intern!(py, "try_autoscale"), (python_bundles,))?;
            Ok(())
        })?;

        // Scaling up should immediately allow workers on recently retired nodes to be re-created,
        // and force a refresh so we can observe newly provisioned nodes quickly.
        state.pending_release_blacklist.clear();
        state.last_refresh = None;

        // 6. Record this request as the new high-water mark so the next cycle will
        //    request exactly one bundle more, and so we never send a smaller request.
        state.max_resources_requested =
            ResourceRequest::try_new_internal(Some(cpu_sum), Some(gpu_sum), Some(memory_sum))?;
        state.last_autoscale_request_time = Some(Instant::now());

        Ok(())
    }

    fn retire_idle_workers(
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
