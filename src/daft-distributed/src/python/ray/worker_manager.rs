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
const DAFT_AUTOSCALE_STRATEGY_ENV: &str = "DAFT_AUTOSCALE_STRATEGY";
const DAFT_AUTOSCALE_BISECT_TIMEOUT_ENV: &str = "DAFT_AUTOSCALE_BISECT_TIMEOUT_SECS";
const DEFAULT_BISECT_TIMEOUT_SECS: u64 = 30;

/// Autoscale strategy selection.
#[derive(Debug, Clone, PartialEq)]
enum AutoscaleStrategy {
    /// Current behavior: each cycle requests exactly one bundle more than previous high-water mark.
    Gradual,
    /// Binary-search/halving: request all demand first, halve on rejection, O(log N) convergence.
    Bisect,
}

/// State tracking for the bisect autoscale strategy.
#[derive(Debug, Clone)]
struct BisectState {
    /// Cluster CPU count when we last issued a request.
    cluster_cpus_at_last_request: f64,
    /// Total CPUs in our last request to Ray.
    last_requested_cpus: f64,
    /// When we issued the last request.
    last_request_time: Instant,
    /// How long to wait before concluding request was rejected.
    growth_timeout: Duration,
}

struct RayWorkerManagerState {
    ray_workers: HashMap<WorkerId, RaySwordfishWorker>,
    last_refresh: Option<Instant>,
    max_resources_requested: ResourceRequest,
    last_autoscale_request_time: Option<Instant>,
    autoscale_interval_secs: Duration,
    strategy: AutoscaleStrategy,
    bisect_state: Option<BisectState>,
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
                last_autoscale_request_time: None,
                autoscale_interval_secs: Duration::from_secs(
                    std::env::var(RAY_AUTOSCALER_UPDATE_INTERVAL_ENV)
                        .ok()
                        .and_then(|val| val.parse::<u64>().ok())
                        .unwrap_or(DEFAULT_AUTOSCALE_INTERVAL_SECS),
                ),
                strategy: match std::env::var(DAFT_AUTOSCALE_STRATEGY_ENV)
                    .unwrap_or_default()
                    .to_lowercase()
                    .as_str()
                {
                    "bisect" => AutoscaleStrategy::Bisect,
                    _ => AutoscaleStrategy::Gradual,
                },
                bisect_state: None,
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
    fn try_autoscale_bisect(
        state: &mut RayWorkerManagerState,
        bundles: Vec<TaskResourceRequest>,
    ) -> DaftResult<()> {
        if bundles.is_empty() {
            return Ok(());
        }

        // Calculate current cluster CPU capacity
        let current_cluster_cpus: f64 =
            state.ray_workers.values().map(|w| w.total_num_cpus()).sum();

        // Calculate total pending demand
        let total_pending_cpus: f64 = bundles
            .iter()
            .map(|b| b.resource_request.num_cpus().unwrap_or(0.0))
            .sum();

        let request_cpus: f64 = match &state.bisect_state {
            None => {
                // First call ever: request all pending demand
                tracing::info!(
                    target: "daft_distributed::autoscale",
                    "Bisect autoscale: initial request for all pending demand ({:.0} CPUs)",
                    total_pending_cpus
                );
                total_pending_cpus
            }
            Some(bisect) => {
                let elapsed = bisect.last_request_time.elapsed();
                let cluster_grew = current_cluster_cpus > bisect.cluster_cpus_at_last_request;

                if cluster_grew {
                    // Last request succeeded (cluster grew) -> greedily request all remaining demand
                    tracing::info!(
                        target: "daft_distributed::autoscale",
                        "Bisect autoscale: cluster grew ({:.0} -> {:.0} CPUs), requesting all remaining demand ({:.0} CPUs)",
                        bisect.cluster_cpus_at_last_request,
                        current_cluster_cpus,
                        total_pending_cpus
                    );
                    total_pending_cpus
                } else if elapsed >= bisect.growth_timeout {
                    // Timeout with no growth -> request was rejected -> halve
                    let halved = bisect.last_requested_cpus / 2.0;
                    let min_bundle_cpus = bundles
                        .first()
                        .and_then(|b| b.resource_request.num_cpus())
                        .unwrap_or(1.0);
                    let result = halved.max(min_bundle_cpus);
                    tracing::warn!(
                        target: "daft_distributed::autoscale",
                        "Bisect autoscale: no growth after {:.0}s, halving request from {:.0} to {:.0} CPUs",
                        elapsed.as_secs_f64(),
                        bisect.last_requested_cpus,
                        result
                    );
                    result
                } else if elapsed < state.autoscale_interval_secs {
                    // Less than one autoscaler cycle since last request, wait
                    return Ok(());
                } else {
                    // Within growth_timeout window, not yet timed out -> keep waiting
                    return Ok(());
                }
            }
        };

        // Select bundles up to request_cpus
        let mut cpu_sum = 0.0;
        let mut selected_bundles = Vec::new();
        for bundle in &bundles {
            cpu_sum += bundle.resource_request.num_cpus().unwrap_or(0.0);
            selected_bundles.push(bundle);
            if cpu_sum >= request_cpus {
                break;
            }
        }

        // Send request to Ray
        Self::send_bundles_to_ray(&selected_bundles)?;

        // Update bisect state
        let growth_timeout = Duration::from_secs(
            std::env::var(DAFT_AUTOSCALE_BISECT_TIMEOUT_ENV)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_BISECT_TIMEOUT_SECS),
        );
        state.bisect_state = Some(BisectState {
            cluster_cpus_at_last_request: current_cluster_cpus,
            last_requested_cpus: cpu_sum,
            last_request_time: Instant::now(),
            growth_timeout,
        });

        Ok(())
    }
}
