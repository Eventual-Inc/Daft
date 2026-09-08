use std::{
    collections::{HashMap, HashSet},
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
const DEFAULT_BISECT_TIMEOUT_SECS: u64 = 30;

/// Autoscale strategy selection.
#[derive(Debug, Clone, PartialEq)]
enum AutoscaleStrategy {
    /// Current behavior: each cycle requests exactly one bundle more than previous high-water mark.
    Gradual,
    /// Binary-search/halving: request all demand first, halve on rejection, O(log N) convergence.
    Bisect,
}

impl AutoscaleStrategy {
    fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "gradual" => Some(Self::Gradual),
            "bisect" => Some(Self::Bisect),
            _ => None,
        }
    }
}

/// State tracking for the bisect autoscale strategy.
#[derive(Debug, Clone)]
struct BisectState {
    /// Cluster CPU count when we last issued a request.
    cluster_cpus_at_last_request: f64,
    /// Cluster GPU count when we last issued a request.
    cluster_gpus_at_last_request: f64,
    /// Cluster memory bytes when we last issued a request.
    cluster_memory_at_last_request: usize,
    /// Total CPUs in our last request to Ray.
    last_requested_cpus: f64,
    /// Total GPUs in our last request to Ray.
    last_requested_gpus: f64,
    /// Total memory bytes in our last request to Ray.
    last_requested_memory: usize,
    /// When we issued the last request.
    last_request_time: Instant,
    /// Worker IDs present when we last issued a request. Used to detect growth even when
    /// total resources stay flat (e.g. a worker dies while an identically-sized one joins,
    /// which is common in homogeneous clusters).
    worker_ids_at_last_request: HashSet<WorkerId>,
}

struct RayWorkerManagerState {
    ray_workers: HashMap<WorkerId, RaySwordfishWorker>,
    last_refresh: Option<Instant>,
    max_resources_requested: ResourceRequest,
    pending_release_blacklist: HashMap<WorkerId, Instant>,
    last_autoscale_request_time: Option<Instant>,
    autoscale_interval_secs: Duration,
    worker_startup_timeout: usize,
    strategy: AutoscaleStrategy,
    bisect_state: Option<BisectState>,
    bisect_growth_timeout: Duration,
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
                    (existing_worker_ids, self.worker_startup_timeout),
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
    /// Create a new manager. Autoscale behavior is configured via `daft.set_runner_ray()`
    /// arguments; unset options fall back to defaults (gradual strategy, 30s bisect timeout).
    pub fn new(
        worker_startup_timeout: usize,
        autoscale_strategy: Option<&str>,
        autoscale_bisect_timeout_secs: Option<u64>,
    ) -> DaftResult<Self> {
        let strategy = match autoscale_strategy {
            Some(value) => AutoscaleStrategy::parse(value).ok_or_else(|| {
                DaftError::ValueError(format!(
                    "Invalid autoscale_strategy '{value}'. Expected 'gradual' or 'bisect'."
                ))
            })?,
            None => AutoscaleStrategy::Gradual,
        };
        let bisect_growth_timeout = Duration::from_secs(
            autoscale_bisect_timeout_secs.unwrap_or(DEFAULT_BISECT_TIMEOUT_SECS),
        );
        Ok(Self {
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
                worker_startup_timeout,
                strategy,
                bisect_state: None,
                bisect_growth_timeout,
            })),
        })
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

    fn cleanup_shuffle_dirs(
        &self,
        dirs: Vec<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DaftResult<()>> + Send + '_>> {
        Box::pin(super::clear_shuffle_dirs_on_all_nodes(dirs))
    }

    /// Autoscale the Ray cluster by requesting resources from Ray's autoscaler.
    fn try_autoscale(&self, bundles: Vec<TaskResourceRequest>) -> DaftResult<()> {
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        match state.strategy.clone() {
            AutoscaleStrategy::Gradual => Self::try_autoscale_gradual(&mut state, bundles),
            AutoscaleStrategy::Bisect => Self::try_autoscale_bisect(&mut state, bundles),
        }
    }

    fn retire_idle_workers(
        &self,
        skip_due_to_pending_scale_up: bool,
        force_all_when_cluster_idle: bool,
    ) -> DaftResult<usize> {
        // 1. Read downscale configuration from the environment. The worker manager owns
        //    every gating decision so the scheduler can stay backend-agnostic.
        //
        //    - `DAFT_AUTOSCALING_DOWNSCALE_ENABLED`: Enables the downscaling feature.
        //      "1" or "true" (case-insensitive) enables it. Defaults to false.
        //    - `DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS`: Minimum number of workers to keep
        //      running even if they are idle. Prevents brief idle periods from collapsing
        //      the cluster to zero. Defaults to 1.
        let downscale_enabled = std::env::var("DAFT_AUTOSCALING_DOWNSCALE_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if !downscale_enabled {
            return Ok(0);
        }

        let min_survivor_workers: usize = std::env::var("DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1);

        // 2. Final-shutdown sweep clears any lingering autoscaling demand even when no
        //    workers end up retired this cycle.
        if force_all_when_cluster_idle {
            Python::attach(|py| -> DaftResult<()> {
                let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
                flotilla_module.call_method0(pyo3::intern!(py, "clear_autoscaling_requests"))?;
                Ok(())
            })?;
        }

        // 3. During an active scale-up, skip downscale so we don't undo demand we just
        //    sent to Ray's autoscaler.
        if skip_due_to_pending_scale_up && !force_all_when_cluster_idle {
            return Ok(0);
        }

        // 4. Determine how many workers we are allowed to retire while honoring the
        //    `min_survivor_workers` floor. The shutdown path bypasses the floor.
        let allowed_to_retire = {
            let state = self
                .state
                .lock()
                .expect("Failed to lock RayWorkerManagerState");
            let num_workers = state.ray_workers.len();
            if force_all_when_cluster_idle {
                num_workers
            } else {
                num_workers.saturating_sub(min_survivor_workers)
            }
        };
        if allowed_to_retire == 0 {
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
                candidates.into_iter().take(allowed_to_retire).collect();

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

impl RayWorkerManager {
    /// Build Python bundle dicts and send to Ray's autoscaler via request_resources().
    fn send_bundles_to_ray(selected_bundles: &[&TaskResourceRequest]) -> DaftResult<()> {
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
        })
    }

    /// Gradual autoscale strategy.
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
    fn try_autoscale_gradual(
        state: &mut RayWorkerManagerState,
        bundles: Vec<TaskResourceRequest>,
    ) -> DaftResult<()> {
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
        Self::send_bundles_to_ray(&selected_bundles)?;

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

    /// Bisect autoscale strategy.
    ///
    /// Algorithm: request all pending demand initially. If the cluster does not grow within
    /// `growth_timeout`, assume the request was rejected (exceeded cluster ceiling) and halve
    /// the request. When the cluster does grow, greedily re-request all remaining demand.
    /// This converges on the cluster's actual capacity in O(log N) steps.
    ///
    /// Tracks CPU, GPU, and memory dimensions to handle zero-CPU bundles (e.g., pure GPU tasks).
    fn try_autoscale_bisect(
        state: &mut RayWorkerManagerState,
        bundles: Vec<TaskResourceRequest>,
    ) -> DaftResult<()> {
        if bundles.is_empty() {
            return Ok(());
        }

        // Calculate current cluster capacity across all dimensions
        let current_cluster_cpus: f64 =
            state.ray_workers.values().map(|w| w.total_num_cpus()).sum();
        let current_cluster_gpus: f64 =
            state.ray_workers.values().map(|w| w.total_num_gpus()).sum();
        let current_cluster_memory: usize = state
            .ray_workers
            .values()
            .map(|w| w.total_memory_bytes())
            .sum();

        // Calculate total pending demand across all dimensions
        let total_pending_cpus: f64 = bundles
            .iter()
            .map(|b| b.resource_request.num_cpus().unwrap_or(0.0))
            .sum();
        let total_pending_gpus: f64 = bundles
            .iter()
            .map(|b| b.resource_request.num_gpus().unwrap_or(0.0))
            .sum();
        let total_pending_memory: usize = bundles
            .iter()
            .map(|b| b.resource_request.memory_bytes().unwrap_or(0))
            .sum();

        // Determine how much to request in each dimension
        let (request_cpus, request_gpus, request_memory): (f64, f64, usize) = match &state
            .bisect_state
        {
            None => {
                // First call ever: request all pending demand
                tracing::info!(
                    target: "daft_distributed::autoscale",
                    "Bisect autoscale: initial request for all pending demand ({:.0} CPUs, {:.0} GPUs, {} bytes memory)",
                    total_pending_cpus,
                    total_pending_gpus,
                    total_pending_memory
                );
                (total_pending_cpus, total_pending_gpus, total_pending_memory)
            }
            Some(bisect) => {
                let elapsed = bisect.last_request_time.elapsed();
                // Also treat new worker IDs as growth: in homogeneous clusters a worker can die
                // while a same-sized replacement joins, leaving resource totals unchanged.
                let has_new_workers = state
                    .ray_workers
                    .keys()
                    .any(|id| !bisect.worker_ids_at_last_request.contains(id));
                let cluster_grew = has_new_workers
                    || current_cluster_cpus > bisect.cluster_cpus_at_last_request
                    || current_cluster_gpus > bisect.cluster_gpus_at_last_request
                    || current_cluster_memory > bisect.cluster_memory_at_last_request;

                if cluster_grew {
                    // Last request succeeded (cluster grew) -> greedily request all remaining demand
                    tracing::info!(
                        target: "daft_distributed::autoscale",
                        "Bisect autoscale: cluster grew (new workers: {}, CPUs {:.0}->{:.0}, GPUs {:.0}->{:.0}, mem {}->{} bytes), requesting all remaining demand",
                        has_new_workers,
                        bisect.cluster_cpus_at_last_request,
                        current_cluster_cpus,
                        bisect.cluster_gpus_at_last_request,
                        current_cluster_gpus,
                        bisect.cluster_memory_at_last_request,
                        current_cluster_memory
                    );
                    (total_pending_cpus, total_pending_gpus, total_pending_memory)
                } else if elapsed >= state.bisect_growth_timeout {
                    // Timeout with no growth -> request was rejected -> halve
                    let halved_cpus = bisect.last_requested_cpus / 2.0;
                    let halved_gpus = bisect.last_requested_gpus / 2.0;
                    let halved_memory = bisect.last_requested_memory / 2;

                    // Ensure we don't go below the first bundle's requirements
                    let first = bundles.first().map(|b| &b.resource_request);
                    let min_cpus = first.and_then(|r| r.num_cpus()).unwrap_or(0.0);
                    let min_gpus = first.and_then(|r| r.num_gpus()).unwrap_or(0.0);
                    let min_memory = first.and_then(|r| r.memory_bytes()).unwrap_or(0);

                    let result_cpus = halved_cpus.max(min_cpus);
                    let result_gpus = halved_gpus.max(min_gpus);
                    let result_memory = halved_memory.max(min_memory);

                    tracing::warn!(
                        target: "daft_distributed::autoscale",
                        "Bisect autoscale: no growth after {:.0}s, halving request (CPUs {:.0}->{:.0}, GPUs {:.0}->{:.0}, mem {}->{} bytes)",
                        elapsed.as_secs_f64(),
                        bisect.last_requested_cpus,
                        result_cpus,
                        bisect.last_requested_gpus,
                        result_gpus,
                        bisect.last_requested_memory,
                        result_memory
                    );
                    (result_cpus, result_gpus, result_memory)
                } else if elapsed < state.autoscale_interval_secs {
                    // Less than one autoscaler cycle since last request, wait
                    return Ok(());
                } else {
                    // Within growth_timeout window, not yet timed out -> keep waiting
                    return Ok(());
                }
            }
        };

        // Select bundles until all non-zero dimensions are satisfied
        let mut cpu_sum = 0.0;
        let mut gpu_sum = 0.0;
        let mut memory_sum: usize = 0;
        let mut selected_bundles = Vec::new();
        for bundle in &bundles {
            cpu_sum += bundle.resource_request.num_cpus().unwrap_or(0.0);
            gpu_sum += bundle.resource_request.num_gpus().unwrap_or(0.0);
            memory_sum += bundle.resource_request.memory_bytes().unwrap_or(0);
            selected_bundles.push(bundle);
            // Break when we've accumulated enough in ALL non-zero dimensions
            let cpu_satisfied = request_cpus <= 0.0 || cpu_sum >= request_cpus;
            let gpu_satisfied = request_gpus <= 0.0 || gpu_sum >= request_gpus;
            let memory_satisfied = request_memory == 0 || memory_sum >= request_memory;
            if cpu_satisfied && gpu_satisfied && memory_satisfied {
                break;
            }
        }

        // Send request to Ray
        Self::send_bundles_to_ray(&selected_bundles)?;

        // Scaling up should immediately allow workers on recently retired nodes to be re-created,
        // and force a refresh so we can observe newly provisioned nodes quickly.
        state.pending_release_blacklist.clear();
        state.last_refresh = None;

        // Update bisect state
        state.bisect_state = Some(BisectState {
            cluster_cpus_at_last_request: current_cluster_cpus,
            cluster_gpus_at_last_request: current_cluster_gpus,
            cluster_memory_at_last_request: current_cluster_memory,
            last_requested_cpus: cpu_sum,
            last_requested_gpus: gpu_sum,
            last_requested_memory: memory_sum,
            last_request_time: Instant::now(),
            worker_ids_at_last_request: state.ray_workers.keys().cloned().collect(),
        });

        Ok(())
    }
}
