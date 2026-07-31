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
    worker::{Worker, WorkerId, WorkerManager},
};

const REFRESH_INTERVAL_SECS: Duration = Duration::from_secs(5);
const DEFAULT_AUTOSCALE_INTERVAL_SECS: u64 = 5;
// Environment variable Ray itself reads to configure its autoscaler reconciliation period.
// We read the same variable so our rate-limit matches Ray's actual cycle length.
const RAY_AUTOSCALER_UPDATE_INTERVAL_ENV: &str = "AUTOSCALER_UPDATE_INTERVAL_S";

struct RayWorkerManagerState {
    ray_workers: HashMap<WorkerId, RaySwordfishWorker>,
    // Workers marked by the reaper as draining: still alive (and counted toward the
    // min-survivor floor) but hidden from `worker_snapshots()` so the scheduler stops
    // assigning new work to them. Released on a later reaper tick if still idle.
    draining_workers: HashSet<WorkerId>,
    last_refresh: Option<Instant>,
    max_resources_requested: ResourceRequest,
    pending_release_blacklist: HashMap<WorkerId, Instant>,
    last_autoscale_request_time: Option<Instant>,
    autoscale_interval_secs: Duration,
    worker_startup_timeout: usize,
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
    pub fn new(worker_startup_timeout: usize) -> Self {
        let state = Arc::new(Mutex::new(RayWorkerManagerState {
            ray_workers: HashMap::new(),
            draining_workers: HashSet::new(),
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
        // Retirement is two-phase (drain, then release on a later tick) so a worker is
        // only ever killed after it has been hidden from scheduler snapshots for at least
        // one full reaper interval — see `scheduling::downscale` for the race analysis.
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
                    let tick_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || Self::reap_idle_workers(&state),
                    ));
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

        Self { state }
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

        // Draining workers are deliberately hidden from the scheduler so no new tasks
        // are assigned to them while the reaper decides whether to release them.
        Ok(state
            .ray_workers
            .values()
            .filter(|w| !state.draining_workers.contains(w.id()))
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
        state.draining_workers.remove(&worker_id);
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
        // and force a refresh so we can observe newly provisioned nodes quickly. Demand is
        // rising, so also put any draining workers back in service immediately instead of
        // letting the reaper release capacity we are about to need.
        state.pending_release_blacklist.clear();
        state.draining_workers.clear();
        state.last_refresh = None;

        // 6. Record this request as the new high-water mark so the next cycle will
        //    request exactly one bundle more, and so we never send a smaller request.
        state.max_resources_requested =
            ResourceRequest::try_new_internal(Some(cpu_sum), Some(gpu_sum), Some(memory_sum))?;
        state.last_autoscale_request_time = Some(Instant::now());

        Ok(())
    }

    fn clear_autoscale_demand(&self) -> DaftResult<()> {
        // Tell Ray's autoscaler to stop provisioning capacity for a job that has
        // finished. This is demand-clearing only — it does not retire any workers.
        // Draining the idle warm pool is owned by the background reaper so retirement
        // has a single authority (see #5683).
        {
            let mut state = self
                .state
                .lock()
                .expect("Failed to lock RayWorkerManagerState");
            // If this job never sent a scale-up request, there is no demand of ours in
            // Ray's autoscaler to clear. Skipping the call avoids touching Python on the
            // default (non-autoscaling) path and avoids clobbering the cluster-wide
            // `request_resources` slot that another job may be using.
            if state.last_autoscale_request_time.is_none() {
                return Ok(());
            }
            state.max_resources_requested = ResourceRequest::default();
            // Also reset the scale-up guard: with no outstanding demand there is nothing
            // for the reaper's guard to protect, so idle workers can drain on schedule.
            state.last_autoscale_request_time = None;
        }

        Python::attach(|py| -> DaftResult<()> {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            flotilla_module.call_method0(pyo3::intern!(py, "clear_autoscaling_requests"))?;
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
    fn reap_idle_workers(state_arc: &Arc<Mutex<RayWorkerManagerState>>) -> DaftResult<usize> {
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

            // Only reset the autoscale high-water mark and force a worker refresh when
            // we actually retired something; a no-op tick must not perturb shared
            // autoscaling state.
            if !workers_to_release.is_empty() {
                state.max_resources_requested = ResourceRequest::default();
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
                "Downscale: marked idle workers as draining (hidden from scheduler)"
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
