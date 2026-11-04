use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, worker::RaySwordfishWorker};
use crate::scheduling::{
    downscale::{
        DEFAULT_REAPER_INTERVAL_SECONDS, DownscalePolicy, REAPER_INTERVAL_SECONDS_ENV,
        WorkerStatus, plan_reap,
    },
    scheduler::WorkerSnapshot,
    task::{SwordfishTask, TaskContext, TaskResourceRequest},
    worker::{AutoscaleDemandId, DispatchLifecycleGuard, Worker, WorkerId, WorkerManager},
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
    /// Worker IDs present when we last issued a request. This supplements resource totals for
    /// workers that do not advertise CPU, GPU, or memory resources. A replacement worker alone
    /// is not considered growth; the current set must strictly extend this snapshot.
    worker_ids_at_last_request: HashSet<WorkerId>,
}

fn worker_set_grew(
    previous_worker_ids: &HashSet<WorkerId>,
    current_worker_ids: &HashSet<WorkerId>,
) -> bool {
    // A new ID can indicate either scale-up or a same-capacity replacement. Only use worker
    // identity as a growth signal when every previously observed worker is still present and the
    // set has strictly expanded. Resource growth remains authoritative when workers are replaced.
    current_worker_ids.len() > previous_worker_ids.len()
        && current_worker_ids.is_superset(previous_worker_ids)
}

fn cluster_capacity_grew(
    bisect: &BisectState,
    current_worker_ids: &HashSet<WorkerId>,
    current_cluster_cpus: f64,
    current_cluster_gpus: f64,
    current_cluster_memory: usize,
) -> bool {
    worker_set_grew(&bisect.worker_ids_at_last_request, current_worker_ids)
        || current_cluster_cpus > bisect.cluster_cpus_at_last_request
        || current_cluster_gpus > bisect.cluster_gpus_at_last_request
        || current_cluster_memory > bisect.cluster_memory_at_last_request
}

/// The autoscaling demand currently held by one owner (one running plan).
///
/// Ray's `request_resources` is a single cluster-wide slot that each call *replaces*,
/// so the manager keeps one of these per owner and always publishes the concatenation
/// of every live owner's `bundles`. Ending one plan then cannot cancel the capacity
/// another plan is still waiting on.
#[derive(Debug, Default)]
struct AutoscaleDemand {
    /// The bundles this owner last asked Ray for, in `request_resources` shape.
    bundles: Vec<HashMap<&'static str, i64>>,
    /// High-water mark of what this owner has requested so far. The request grows by one
    /// bundle per autoscaler cycle, so this is ramp state and it is deliberately
    /// per-owner: a newly started plan must not inherit an older plan's ramp.
    high_water_mark: ResourceRequest,
    /// When this owner last published, used to rate-limit its ramp to Ray's cycle.
    last_request_time: Option<Instant>,
    /// Ramp state for the [`AutoscaleStrategy::Bisect`] strategy, kept per owner for the
    /// same reason as `high_water_mark`: bisect halves *this* owner's request on a growth
    /// timeout and must never drive another owner's, and each owner writes only its own
    /// slice into the published union.
    bisect: Option<BisectState>,
}

struct RayWorkerManagerState {
    ray_workers: HashMap<WorkerId, RaySwordfishWorker>,
    // Workers marked by the reaper as draining: still alive (and counted toward the
    // min-survivor floor) but flagged in `worker_snapshots()` so the scheduler stops
    // assigning them new work. Released on a later reaper tick if still idle.
    draining_workers: HashSet<WorkerId>,
    last_refresh: Option<Instant>,
    /// Live autoscaling demand, keyed by the plan that asked for it.
    autoscale_demands: HashMap<AutoscaleDemandId, AutoscaleDemand>,
    pending_release_blacklist: HashMap<WorkerId, Instant>,
    last_autoscale_request_time: Option<Instant>,
    autoscale_interval_secs: Duration,
    worker_startup_timeout: usize,
    strategy: AutoscaleStrategy,
    bisect_growth_timeout: Duration,
}

impl RayWorkerManagerState {
    /// The bundles to publish to Ray: the concatenation of every live owner's demand.
    ///
    /// `request_resources` replaces the cluster-wide demand on every call, so this must
    /// always be the full picture. An empty result is the "no demand" request, i.e. what
    /// `clear_autoscaling_requests()` sends.
    fn all_autoscale_bundles(&self) -> Vec<HashMap<&'static str, i64>> {
        self.autoscale_demands
            .values()
            .flat_map(|demand| demand.bundles.iter().cloned())
            .collect()
    }

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
    dispatch_gate: Arc<Mutex<()>>,
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
        let bisect_growth_timeout_secs =
            autoscale_bisect_timeout_secs.unwrap_or(DEFAULT_BISECT_TIMEOUT_SECS);
        if bisect_growth_timeout_secs == 0 {
            return Err(DaftError::ValueError(
                "autoscale_bisect_timeout_secs must be greater than zero".to_string(),
            ));
        }
        let bisect_growth_timeout = Duration::from_secs(bisect_growth_timeout_secs);

        let state = Arc::new(Mutex::new(RayWorkerManagerState {
            ray_workers: HashMap::new(),
            draining_workers: HashSet::new(),
            last_refresh: None,
            autoscale_demands: HashMap::new(),
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
            bisect_growth_timeout,
        }));

        // Background reaper: the single authority for retiring idle workers. The scheduler
        // no longer retires workers at all, which avoids two actors racing over the same
        // pool. This detached thread runs on its own timer, independent of query
        // boundaries, so genuinely idle workers past the min-survivor floor are drained
        // whether they went idle mid-query, between queries, or after the final query of a
        // session — cases the per-query scheduler loop could never all cover. Workers idle
        // for less than the threshold stay warm for the next query. It is a no-op unless
        // downscaling is enabled, and it holds a Weak handle so it self-terminates once the
        // manager is dropped.
        //
        // Always acquire the dispatch gate before state locks or Python attachment.
        let dispatch_gate = Arc::new(Mutex::new(()));
        let reaper_dispatch_gate = dispatch_gate.clone();
        let reaper_state = Arc::downgrade(&state);
        let spawn_result = std::thread::Builder::new()
            .name("daft-idle-reaper".to_string())
            .spawn(move || {
                loop {
                    // Re-read every tick, matching how the downscale policy env vars are
                    // re-read per tick, so the cadence can be tuned at runtime.
                    let interval = std::env::var(REAPER_INTERVAL_SECONDS_ENV)
                        .ok()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(DEFAULT_REAPER_INTERVAL_SECONDS)
                        .max(1);
                    std::thread::sleep(Duration::from_secs(interval));
                    let Some(state) = reaper_state.upgrade() else {
                        break;
                    };
                    // The reaper is a detached thread with no supervisor: a stray panic
                    // (including lock poisoning from another thread) must not silently
                    // kill retirement for the rest of the session.
                    let tick_result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            Self::reap_idle_workers(&state, &reaper_dispatch_gate)
                        }));
                    match tick_result {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            tracing::warn!(
                                target: "ray_worker_manager",
                                error = %e,
                                "Background idle reaper tick failed"
                            );
                        }
                        Err(_) => {
                            tracing::error!(
                                target: "ray_worker_manager",
                                "Background idle reaper tick panicked; continuing"
                            );
                        }
                    }
                }
            });
        if let Err(e) = spawn_result {
            tracing::error!(
                target: "ray_worker_manager",
                error = %e,
                "Failed to spawn idle reaper thread; idle workers will not be retired"
            );
        }

        Ok(Self {
            state,
            dispatch_gate,
        })
    }

    /// Lock the shared state, recovering from poisoning. Used on the reaper path so a
    /// panic elsewhere cannot permanently disable retirement.
    fn lock_state(
        state_arc: &Arc<Mutex<RayWorkerManagerState>>,
    ) -> MutexGuard<'_, RayWorkerManagerState> {
        state_arc
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl WorkerManager for RayWorkerManager {
    type Worker = RaySwordfishWorker;

    fn begin_dispatch(&self) -> DispatchLifecycleGuard<'_> {
        DispatchLifecycleGuard::new(
            self.dispatch_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

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

        // Draining workers stay visible but are tagged, so the scheduler stops giving
        // them discretionary work while hard-affinity tasks that can only run there can
        // still resolve their target. Hiding them outright made such tasks permanently
        // unschedulable: the hard-affinity path has no fallback and simply fails when
        // the worker is absent from the snapshots.
        Ok(state
            .ray_workers
            .values()
            .map(|w| WorkerSnapshot::from(w).with_draining(state.draining_workers.contains(w.id())))
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
        state.draining_workers.remove(&worker_id);
    }

    fn shutdown(&self) -> DaftResult<()> {
        let state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        Python::attach(|py| {
            for worker in state.ray_workers.values() {
                // Best effort: a failure to tear one actor down must not abort the
                // shutdown of the remaining workers.
                if let Err(e) = worker.shutdown(py) {
                    tracing::error!(
                        target: "ray_worker_manager",
                        worker_id = %worker.id(),
                        error = %e,
                        "Failed to shut down worker during teardown"
                    );
                }
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

    /// Autoscale the Ray cluster by requesting resources from Ray's autoscaler on behalf of
    /// one owner (one running plan).
    ///
    /// Constraints we operate under:
    /// - There is no reliable programmatic way for Daft to know the cluster's true autoscaling
    ///   ceiling ahead of time (for example, KubeRay `maxReplicas` or other external limits).
    /// - Daft can only observe currently registered Ray workers; it cannot directly account for
    ///   capacity that has already been requested but is still provisioning.
    /// - `ray.autoscaler.sdk.request_resources(bundles=...)` is **asynchronous** and each call
    ///   **replaces** a single cluster-wide demand slot (it is not additive). We therefore keep
    ///   one [`AutoscaleDemand`] per owner and always publish the union of every live owner's
    ///   bundles, so one plan can never clobber the capacity another is still waiting on.
    /// - Ray's autoscaler reconciliation loop processes the request every ~5 seconds by default
    ///   (configurable via `AUTOSCALER_UPDATE_INTERVAL_S`). Calls between cycles overwrite
    ///   each other — only the latest value at reconciliation time is processed.
    /// - If the requested bundles exceed the cluster's maximum capacity (e.g., KubeRay
    ///   `maxReplicas`), the autoscaler refuses to scale **at all** — not even partially.
    /// - We cannot detect whether the Ray autoscaler accepted or rejected the request, and
    ///   observing new workers is not a reliable signal for whether a request succeeded, since
    ///   node provisioning time varies (seconds to minutes depending on the environment).
    ///
    /// Two ramp shapes are available (see [`AutoscaleStrategy`]). `Gradual` (the default, handled
    /// inline below) sends one more bundle than this owner's previous high-water mark each cycle.
    /// `Bisect` (see [`Self::try_autoscale_bisect`]) requests all of the owner's demand, then
    /// halves it on a growth timeout, converging in O(log N). Both are per-owner and publish into
    /// the shared union.
    fn try_autoscale(
        &self,
        demand_id: AutoscaleDemandId,
        bundles: Vec<TaskResourceRequest>,
    ) -> DaftResult<()> {
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        // Bisect is a different ramp shape and lives in its own routine; dispatch to it before
        // the gradual path below. Both write only this owner's slice and then publish the union
        // of every live owner's demand, so neither clobbers a concurrent plan in Ray's single
        // cluster-wide `request_resources` slot.
        if matches!(state.strategy, AutoscaleStrategy::Bisect) {
            return Self::try_autoscale_bisect(&mut state, demand_id, bundles);
        }

        // Gradual strategy (default): ramp this owner's request up by exactly one bundle per Ray
        // autoscaler cycle, tracked as a per-owner high-water mark.

        // 1. Only attempt to grow this owner's request once per Ray autoscaler
        //    reconciliation cycle. Sending more frequently would just overwrite the
        //    previous value before Ray processes it. The rate limit is per owner: a
        //    plan that just started must not have to wait out another plan's cycle.
        //    Note we deliberately do not register the owner here — an owner that bails
        //    out below never published anything, so it must not appear in the demand map.
        let autoscale_interval = state.autoscale_interval_secs;
        let high_water_mark = match state.autoscale_demands.get(&demand_id) {
            Some(demand) => {
                if let Some(last_time) = demand.last_request_time
                    && last_time.elapsed() < autoscale_interval
                {
                    return Ok(());
                }
                demand.high_water_mark.clone()
            }
            None => ResourceRequest::default(),
        };

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
        let high_water_mark_cpus = high_water_mark
            .num_cpus()
            .unwrap_or(0.0)
            .max(cluster_num_cpus);
        let high_water_mark_gpus = high_water_mark
            .num_gpus()
            .unwrap_or(0.0)
            .max(cluster_num_gpus);
        let high_water_mark_memory = high_water_mark
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

        // 5. Translate the selected bundles into Ray's `request_resources` shape.
        let own_bundles = Self::build_bundle_dicts(&selected_bundles);

        // 6. Record this request as this owner's new high-water mark, so its next cycle
        //    requests exactly one bundle more and never sends a smaller request.
        let own_bundle_count = own_bundles.len();
        let demand = state.autoscale_demands.entry(demand_id).or_default();
        demand.bundles = own_bundles;
        demand.high_water_mark =
            ResourceRequest::try_new_internal(Some(cpu_sum), Some(gpu_sum), Some(memory_sum))?;
        demand.last_request_time = Some(Instant::now());

        // 7. Publish the union of every live owner's demand and apply the shared
        //    post-scale-up state updates.
        Self::publish_union_and_refresh(&mut state, demand_id, own_bundle_count)
    }

    fn clear_autoscale_demand(&self, demand_id: AutoscaleDemandId) -> DaftResult<()> {
        // Tell Ray's autoscaler to stop provisioning capacity for a job that has
        // finished. This is demand-clearing only — it does not retire any workers.
        // Draining the idle warm pool is owned by the background reaper so retirement
        // has a single authority (see #5683).
        let remaining_bundles = {
            let mut state = Self::lock_state(&self.state);
            // If this job never sent a scale-up request, there is no demand of ours in
            // Ray's autoscaler to clear. Skipping the call avoids touching Python on the
            // default (non-autoscaling) path and avoids writing to the cluster-wide
            // `request_resources` slot that another job may be using.
            if state.autoscale_demands.remove(&demand_id).is_none() {
                return Ok(());
            }
            if state.autoscale_demands.is_empty() {
                // Nothing of ours is outstanding any more, so the reaper's scale-up guard
                // has nothing left to protect and idle workers can drain on schedule.
                state.last_autoscale_request_time = None;
            }
            // Re-publish whatever other plans still need. When nothing is left this is an
            // empty request, which is exactly `clear_autoscaling_requests()`.
            state.all_autoscale_bundles()
        };

        Python::attach(|py| -> DaftResult<()> {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            flotilla_module
                .call_method1(pyo3::intern!(py, "try_autoscale"), (remaining_bundles,))?;
            Ok(())
        })?;
        Ok(())
    }
}

impl RayWorkerManager {
    /// Core idle-retirement routine, run exclusively by the background reaper thread.
    /// This is the single retirement authority: the scheduler never retires workers, so
    /// there is no second actor racing over the same pool.
    ///
    /// All policy decisions (enable flag, min-survivor floor, idle threshold, scale-up
    /// guard, two-phase draining) live in `scheduling::downscale::plan_reap`, computed
    /// from a single consistent snapshot of the state taken under one lock so the guard,
    /// floor, and candidate selection cannot diverge. This function only gathers inputs
    /// and applies the plan.
    fn reap_idle_workers(
        state_arc: &Arc<Mutex<RayWorkerManagerState>>,
        dispatch_gate: &Mutex<()>,
    ) -> DaftResult<usize> {
        // Keep retirement, including failed-release reinsertion, outside snapshot-to-submit.
        let _dispatch_guard = match dispatch_gate.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(0),
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
        };
        // Read the downscale policy from the environment on every tick. The worker
        // manager owns every gating decision so the scheduler can stay backend-agnostic.
        let policy = DownscalePolicy::from_env();

        // Cheap early-outs under the lock before touching Python: when downscaling is
        // disabled the reaper must be a pure no-op (aside from restoring any workers
        // left draining if the flag was flipped off mid-drain), and an empty or
        // at-the-floor pool with nothing draining needs no head-node lookup.
        {
            let mut state = Self::lock_state(state_arc);
            if !policy.enabled {
                state.draining_workers.clear();
                return Ok(0);
            }
            if state.draining_workers.is_empty()
                && state.ray_workers.len() <= policy.min_survivor_workers
            {
                return Ok(0);
            }
        }

        // Determine the Ray head node id so we can avoid retiring its worker. Done
        // outside the state lock: never hold the lock across Python/GIL calls.
        let head_node_id: Option<String> = Python::attach(|py| {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            let head_id_obj =
                flotilla_module.call_method0(pyo3::intern!(py, "get_head_node_id"))?;
            let head_id = head_id_obj.extract::<Option<String>>()?;
            DaftResult::Ok(head_id)
        })?;

        // Single critical section: snapshot worker statuses, compute the plan, and apply
        // all state transitions atomically with respect to the scheduler's dispatch path.
        let (workers_to_release, drained, survivors_after, blacklisted_after) = {
            let mut state = Self::lock_state(state_arc);

            // Scale-up guard, derived from our own state. If a scale-up request went to
            // Ray within the last autoscaler cycle, Ray may still be provisioning nodes
            // for it — the plan suppresses retirement (and cancels in-progress drains)
            // rather than undoing demand we just signaled. Note this is deliberately
            // *stronger* than the old scheduler-supplied same-tick flag: retirement is
            // paused for a full autoscaler cycle after every scale-up request.
            let scale_up_in_flight = state
                .last_autoscale_request_time
                .is_some_and(|last_time| last_time.elapsed() < state.autoscale_interval_secs);

            let now = Instant::now();
            let statuses: Vec<WorkerStatus> = state
                .ray_workers
                .values()
                .map(|w| WorkerStatus {
                    worker_id: w.id().clone(),
                    is_head_node: head_node_id.as_deref() == Some(w.id().as_ref()),
                    idle_for: w.is_idle().then(|| w.idle_duration(now)),
                    draining: state.draining_workers.contains(w.id()),
                })
                .collect();

            let plan = plan_reap(&policy, scale_up_in_flight, &statuses);

            for wid in &plan.undrain {
                state.draining_workers.remove(wid);
            }
            for wid in &plan.drain {
                state.draining_workers.insert(wid.clone());
            }

            let mut workers_to_release = Vec::with_capacity(plan.release.len());
            for wid in &plan.release {
                if let Some(worker) = state.ray_workers.remove(wid) {
                    state.draining_workers.remove(wid);
                    state
                        .pending_release_blacklist
                        .insert(wid.clone(), Instant::now());
                    workers_to_release.push(worker);
                }
            }

            // Only force a worker refresh when we actually retired something; a no-op
            // tick must not perturb shared state. Autoscaling demand is deliberately
            // untouched here: it belongs to whichever plans are still running, and the
            // reaper is not one of them.
            if !workers_to_release.is_empty() {
                state.last_refresh = None;
            }

            (
                workers_to_release,
                plan.drain.len(),
                state.ray_workers.len(),
                state.pending_release_blacklist.len(),
            )
        };

        if drained > 0 {
            tracing::info!(
                target: "ray_worker_manager",
                drained,
                "Downscale: marked idle workers as draining (skipped by the scheduler)"
            );
        }

        if workers_to_release.is_empty() {
            return Ok(0);
        }

        tracing::info!(
            target: "ray_worker_manager",
            "Preparing to release {} workers",
            workers_to_release.len()
        );

        let mut released = 0usize;
        // Workers we removed from state but could not actually shut down: they are
        // still alive out there, so they must go back into the manager's state
        // instead of being leaked (invisible to the scheduler, yet consuming cluster
        // resources until the blacklist TTL expires).
        let mut not_released = Vec::new();
        Python::attach(|py| {
            for mut worker in workers_to_release {
                match worker.release(py) {
                    Ok(true) => released += 1,
                    Ok(false) => {
                        // Picked up work between selection and release.
                        not_released.push(worker);
                    }
                    Err(e) => {
                        tracing::error!(
                            target: "ray_worker_manager",
                            worker_id = %worker.id(),
                            error = %e,
                            "Failed to release worker; returning it to the pool"
                        );
                        not_released.push(worker);
                    }
                }
            }
        });

        if !not_released.is_empty() {
            let mut state = Self::lock_state(state_arc);
            for worker in not_released {
                let worker_id = worker.id().clone();
                state.pending_release_blacklist.remove(&worker_id);
                state.draining_workers.remove(&worker_id);
                state.ray_workers.insert(worker_id, worker);
            }
            // The pool changed under us; make the next snapshot re-read it.
            state.last_refresh = None;
        }

        if released == 0 {
            return Ok(0);
        }

        // Note: we deliberately do not touch Ray's autoscaling request here. It is the
        // union of the demand published by every live plan, keyed by owner, and each owner
        // retracts its own slice when it finishes (`clear_autoscale_demand`). Retiring an
        // idle worker does not change that union, and because `request_resources` replaces
        // the cluster-wide slot on every call, clearing it here — as this code used to —
        // would cancel capacity that a still-running plan is waiting on.

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
    /// Translate selected task resource requests into Ray `request_resources` bundle dicts.
    /// Strips zero-valued GPU/memory keys so Ray doesn't interpret them as a demand for
    /// zero-resource bundles on specialized nodes.
    fn build_bundle_dicts(
        selected_bundles: &[&TaskResourceRequest],
    ) -> Vec<HashMap<&'static str, i64>> {
        selected_bundles
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
            .collect()
    }

    /// Publish the union of every live owner's demand to Ray and apply the state updates that
    /// every successful scale-up shares. `request_resources` replaces the cluster-wide slot on
    /// each call, so we must always send the full picture — sending only this owner's bundles
    /// would silently cancel the capacity a concurrently running plan is waiting on.
    fn publish_union_and_refresh(
        state: &mut RayWorkerManagerState,
        demand_id: AutoscaleDemandId,
        own_bundles: usize,
    ) -> DaftResult<()> {
        let published_bundles = state.all_autoscale_bundles();

        tracing::debug!(
            target: "ray_worker_manager",
            demand_id = %demand_id,
            own_bundles,
            total_bundles = published_bundles.len(),
            live_owners = state.autoscale_demands.len(),
            strategy = ?state.strategy,
            "Publishing autoscaling demand"
        );

        Python::attach(|py| -> DaftResult<()> {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            flotilla_module
                .call_method1(pyo3::intern!(py, "try_autoscale"), (published_bundles,))?;
            Ok(())
        })?;

        // Scaling up should immediately allow workers on recently retired nodes to be re-created,
        // and force a refresh so we can observe newly provisioned nodes quickly. Demand is
        // rising, so also put any draining workers back in service immediately instead of
        // letting the reaper release capacity we are about to need.
        state.pending_release_blacklist.clear();
        state.draining_workers.clear();
        state.last_refresh = None;
        // Cluster-wide guard used by the reaper: any recent scale-up, from any owner,
        // suppresses retirement for a full autoscaler cycle.
        state.last_autoscale_request_time = Some(Instant::now());

        Ok(())
    }

    /// Bisect autoscale strategy, tracked per owner.
    ///
    /// Algorithm: request all of this owner's pending demand initially. If the cluster does not
    /// grow within `bisect_growth_timeout`, assume the request was rejected (it exceeded the
    /// cluster ceiling) and halve it. When the cluster does grow, greedily re-request all
    /// remaining demand. This converges on the cluster's actual capacity in O(log N) steps.
    ///
    /// The ramp state lives in the owner's [`AutoscaleDemand`], and the selected bundles are
    /// written to that owner's slice before the union is published, so a concurrent plan's
    /// demand is never clobbered. Tracks CPU, GPU, and memory dimensions to handle zero-CPU
    /// bundles (e.g. pure GPU tasks).
    fn try_autoscale_bisect(
        state: &mut RayWorkerManagerState,
        demand_id: AutoscaleDemandId,
        bundles: Vec<TaskResourceRequest>,
    ) -> DaftResult<()> {
        if bundles.is_empty() {
            return Ok(());
        }

        // Current cluster capacity across all dimensions.
        let current_cluster_cpus: f64 =
            state.ray_workers.values().map(|w| w.total_num_cpus()).sum();
        let current_cluster_gpus: f64 =
            state.ray_workers.values().map(|w| w.total_num_gpus()).sum();
        let current_cluster_memory: usize = state
            .ray_workers
            .values()
            .map(|w| w.total_memory_bytes())
            .sum();

        // This owner's total pending demand across all dimensions.
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

        // This owner's previous bisect state, if it has requested before.
        let prior_bisect = state
            .autoscale_demands
            .get(&demand_id)
            .and_then(|demand| demand.bisect.clone());

        // Determine how much to request in each dimension.
        let (request_cpus, request_gpus, request_memory): (f64, f64, usize) = match prior_bisect {
            None => {
                // First request from this owner: ask for all of its pending demand.
                tracing::info!(
                    target: "daft_distributed::autoscale",
                    demand_id = %demand_id,
                    "Bisect autoscale: initial request for all pending demand ({:.0} CPUs, {:.0} GPUs, {} bytes memory)",
                    total_pending_cpus,
                    total_pending_gpus,
                    total_pending_memory
                );
                (total_pending_cpus, total_pending_gpus, total_pending_memory)
            }
            Some(bisect) => {
                let elapsed = bisect.last_request_time.elapsed();
                let current_worker_ids = state.ray_workers.keys().cloned().collect::<HashSet<_>>();
                let worker_set_grew =
                    worker_set_grew(&bisect.worker_ids_at_last_request, &current_worker_ids);
                let cluster_grew = cluster_capacity_grew(
                    &bisect,
                    &current_worker_ids,
                    current_cluster_cpus,
                    current_cluster_gpus,
                    current_cluster_memory,
                );

                if cluster_grew {
                    // Last request succeeded (cluster grew) -> greedily request all remaining demand.
                    tracing::info!(
                        target: "daft_distributed::autoscale",
                        demand_id = %demand_id,
                        "Bisect autoscale: cluster grew (worker set expanded: {}, CPUs {:.0}->{:.0}, GPUs {:.0}->{:.0}, mem {}->{} bytes), requesting all remaining demand",
                        worker_set_grew,
                        bisect.cluster_cpus_at_last_request,
                        current_cluster_cpus,
                        bisect.cluster_gpus_at_last_request,
                        current_cluster_gpus,
                        bisect.cluster_memory_at_last_request,
                        current_cluster_memory
                    );
                    (total_pending_cpus, total_pending_gpus, total_pending_memory)
                } else if elapsed >= state.bisect_growth_timeout {
                    // Timeout with no growth -> the request was rejected -> halve it.
                    let halved_cpus = bisect.last_requested_cpus / 2.0;
                    let halved_gpus = bisect.last_requested_gpus / 2.0;
                    let halved_memory = bisect.last_requested_memory / 2;

                    // Never halve below the first bundle's requirements.
                    let first = bundles.first().map(|b| &b.resource_request);
                    let min_cpus = first.and_then(|r| r.num_cpus()).unwrap_or(0.0);
                    let min_gpus = first.and_then(|r| r.num_gpus()).unwrap_or(0.0);
                    let min_memory = first.and_then(|r| r.memory_bytes()).unwrap_or(0);

                    let result_cpus = halved_cpus.max(min_cpus);
                    let result_gpus = halved_gpus.max(min_gpus);
                    let result_memory = halved_memory.max(min_memory);

                    tracing::warn!(
                        target: "daft_distributed::autoscale",
                        demand_id = %demand_id,
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
                } else {
                    // Either less than one autoscaler cycle since the last request, or still
                    // within the growth-timeout window: wait and keep observing.
                    return Ok(());
                }
            }
        };

        // Select bundles until all non-zero dimensions are satisfied.
        let mut cpu_sum = 0.0;
        let mut gpu_sum = 0.0;
        let mut memory_sum: usize = 0;
        let mut selected_bundles = Vec::new();
        for bundle in &bundles {
            cpu_sum += bundle.resource_request.num_cpus().unwrap_or(0.0);
            gpu_sum += bundle.resource_request.num_gpus().unwrap_or(0.0);
            memory_sum += bundle.resource_request.memory_bytes().unwrap_or(0);
            selected_bundles.push(bundle);
            // Break once we've accumulated enough in ALL non-zero dimensions.
            let cpu_satisfied = request_cpus <= 0.0 || cpu_sum >= request_cpus;
            let gpu_satisfied = request_gpus <= 0.0 || gpu_sum >= request_gpus;
            let memory_satisfied = request_memory == 0 || memory_sum >= request_memory;
            if cpu_satisfied && gpu_satisfied && memory_satisfied {
                break;
            }
        }

        // Record this owner's slice and its new bisect state, then publish the union.
        let own_bundles = Self::build_bundle_dicts(&selected_bundles);
        let own_bundle_count = own_bundles.len();
        let demand = state.autoscale_demands.entry(demand_id).or_default();
        demand.bundles = own_bundles;
        demand.bisect = Some(BisectState {
            cluster_cpus_at_last_request: current_cluster_cpus,
            cluster_gpus_at_last_request: current_cluster_gpus,
            cluster_memory_at_last_request: current_cluster_memory,
            last_requested_cpus: cpu_sum,
            last_requested_gpus: gpu_sum,
            last_requested_memory: memory_sum,
            last_request_time: Instant::now(),
            worker_ids_at_last_request: state.ray_workers.keys().cloned().collect(),
        });

        Self::publish_union_and_refresh(state, demand_id, own_bundle_count)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    fn worker_ids(ids: &[&str]) -> HashSet<WorkerId> {
        ids.iter().map(|id| Arc::<str>::from(*id)).collect()
    }

    fn bisect_state(worker_ids_at_last_request: HashSet<WorkerId>) -> BisectState {
        BisectState {
            cluster_cpus_at_last_request: 8.0,
            cluster_gpus_at_last_request: 0.0,
            cluster_memory_at_last_request: 1024,
            last_requested_cpus: 16.0,
            last_requested_gpus: 0.0,
            last_requested_memory: 2048,
            last_request_time: Instant::now(),
            worker_ids_at_last_request,
        }
    }

    #[test]
    fn reaper_skips_dispatch_before_locking_state() {
        let manager = RayWorkerManager {
            state: Arc::new(Mutex::new(RayWorkerManagerState {
                ray_workers: HashMap::new(),
                draining_workers: HashSet::new(),
                last_refresh: None,
                autoscale_demands: HashMap::new(),
                pending_release_blacklist: HashMap::new(),
                last_autoscale_request_time: None,
                autoscale_interval_secs: Duration::from_secs(5),
                worker_startup_timeout: 1,
                strategy: AutoscaleStrategy::Gradual,
                bisect_growth_timeout: Duration::from_secs(DEFAULT_BISECT_TIMEOUT_SECS),
            })),
            dispatch_gate: Arc::new(Mutex::new(())),
        };
        let dispatch_guard = manager.begin_dispatch();
        let state_guard = RayWorkerManager::lock_state(&manager.state);
        let state = manager.state.clone();
        let dispatch_gate = manager.dispatch_gate.clone();
        let (tx, rx) = mpsc::channel();
        let reaper = std::thread::spawn(move || {
            tx.send(RayWorkerManager::reap_idle_workers(&state, &dispatch_gate))
                .unwrap();
        });

        let result = rx.recv_timeout(Duration::from_secs(5));
        drop(state_guard);
        drop(dispatch_guard);
        reaper.join().unwrap();
        assert_eq!(result.unwrap().unwrap(), 0);
        assert!(manager.dispatch_gate.try_lock().is_ok());
    }

    #[test]
    fn equal_capacity_worker_replacement_is_not_growth() {
        let bisect = bisect_state(worker_ids(&["worker-a", "worker-b"]));
        let current_worker_ids = worker_ids(&["worker-a", "worker-c"]);

        assert!(!cluster_capacity_grew(
            &bisect,
            &current_worker_ids,
            8.0,
            0.0,
            1024,
        ));
    }

    #[test]
    fn strictly_expanded_worker_set_is_growth() {
        let bisect = bisect_state(worker_ids(&["worker-a", "worker-b"]));
        let current_worker_ids = worker_ids(&["worker-a", "worker-b", "worker-c"]);

        assert!(cluster_capacity_grew(
            &bisect,
            &current_worker_ids,
            8.0,
            0.0,
            1024,
        ));
    }

    #[test]
    fn resource_increase_is_growth_during_worker_replacement() {
        let bisect = bisect_state(worker_ids(&["worker-a", "worker-b"]));
        let current_worker_ids = worker_ids(&["worker-a", "worker-c"]);

        assert!(cluster_capacity_grew(
            &bisect,
            &current_worker_ids,
            12.0,
            0.0,
            1024,
        ));
    }

    #[test]
    fn zero_bisect_timeout_is_rejected() {
        let error = RayWorkerManager::new(120, Some("bisect"), Some(0))
            .err()
            .expect("zero bisect timeout should be rejected");

        assert!(
            error
                .to_string()
                .contains("autoscale_bisect_timeout_secs must be greater than zero")
        );
    }
}
