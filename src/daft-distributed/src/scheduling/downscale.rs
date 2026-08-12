//! Pure decision logic for retiring (scaling down) idle workers.
//!
//! This module is deliberately backend-agnostic and free of any Python/Ray
//! dependencies so the retirement policy can be unit-tested without a live
//! cluster. The backend worker manager (see `RayWorkerManager`) gathers a
//! consistent snapshot of worker statuses under its state lock, calls
//! [`plan_reap`], and applies the returned plan.
//!
//! Retirement is two-phase to avoid racing with the scheduler's
//! snapshot->dispatch window (a worker could be selected for dispatch from a
//! snapshot taken just before retirement):
//!
//! 1. **Drain**: a worker idle past the threshold is marked as draining. A
//!    draining worker stays alive and can still accept tasks, but is flagged as
//!    such in `worker_snapshots()` so the scheduler stops assigning new work to
//!    it wherever it has an alternative. Hard-affinity tasks, which have no
//!    fallback worker, may still be placed there — that immediately makes the
//!    worker non-idle, so the next tick puts it back in service.
//! 2. **Release**: on a later reaper tick, a draining worker that is *still*
//!    idle is actually released. If it picked up work in the meantime (the
//!    race resolving in favor of the task), it is put back in service instead.
//!
//! A release can therefore only happen after the worker has been off the
//! scheduler's list of candidates for at least one full reaper interval, which
//! is orders of magnitude longer than the synchronous snapshot->dispatch span in
//! the scheduler loop.

// The runtime consumer (RayWorkerManager) only exists under the `python` feature;
// without it this module is exercised by unit tests alone.
#![cfg_attr(not(feature = "python"), allow(dead_code))]

use std::time::Duration;

use super::worker::WorkerId;

pub(crate) const DOWNSCALE_ENABLED_ENV: &str = "DAFT_AUTOSCALING_DOWNSCALE_ENABLED";
pub(crate) const DOWNSCALE_IDLE_SECONDS_ENV: &str = "DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS";
pub(crate) const MIN_SURVIVOR_WORKERS_ENV: &str = "DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS";
pub(crate) const REAPER_INTERVAL_SECONDS_ENV: &str = "DAFT_AUTOSCALING_REAPER_INTERVAL_SECONDS";

const DEFAULT_IDLE_SECONDS: u64 = 60;
const DEFAULT_MIN_SURVIVOR_WORKERS: usize = 1;
pub(crate) const DEFAULT_REAPER_INTERVAL_SECONDS: u64 = 5;

/// Downscale configuration, read from the environment by the background reaper
/// on every tick so it can be reconfigured at runtime.
#[derive(Debug, Clone)]
pub(crate) struct DownscalePolicy {
    /// Master switch (`DAFT_AUTOSCALING_DOWNSCALE_ENABLED`). Defaults to false.
    pub enabled: bool,
    /// Minimum number of workers kept alive even when idle
    /// (`DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS`). Prevents brief idle periods
    /// from collapsing the warm pool between queries in a session (#5683).
    pub min_survivor_workers: usize,
    /// Minimum idle time before a worker becomes eligible for retirement
    /// (`DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS`), so workers that were busy
    /// moments ago survive into the next query instead of being rebuilt.
    pub idle_threshold: Duration,
}

impl DownscalePolicy {
    pub fn from_env() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let enabled = get(DOWNSCALE_ENABLED_ENV)
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let min_survivor_workers = get(MIN_SURVIVOR_WORKERS_ENV)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MIN_SURVIVOR_WORKERS);
        let idle_threshold = Duration::from_secs(
            get(DOWNSCALE_IDLE_SECONDS_ENV)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(DEFAULT_IDLE_SECONDS),
        );
        Self {
            enabled,
            min_survivor_workers,
            idle_threshold,
        }
    }
}

/// Point-in-time status of a single worker, gathered under the worker
/// manager's state lock so the whole plan is computed from one consistent
/// snapshot (guard, floor, and candidate selection cannot diverge).
#[derive(Debug, Clone)]
pub(crate) struct WorkerStatus {
    pub worker_id: WorkerId,
    /// The Ray head node's worker is never retired.
    pub is_head_node: bool,
    /// `Some(duration)` if the worker is idle, `None` if it has active tasks.
    pub idle_for: Option<Duration>,
    /// Whether the worker was marked as draining on a previous tick.
    pub draining: bool,
}

/// The state transitions to apply for one reaper tick.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ReapPlan {
    /// Draining workers that stayed idle past the threshold for a full reaper
    /// cycle: safe to release now.
    pub release: Vec<WorkerId>,
    /// Newly idle-past-threshold workers to mark as draining (flagged in
    /// scheduler snapshots, released on a later tick if still idle).
    pub drain: Vec<WorkerId>,
    /// Draining workers to put back in service: they picked up work, downscale
    /// was disabled, a scale-up is in flight, or the floor no longer allows
    /// retiring them.
    pub undrain: Vec<WorkerId>,
}

/// Compute the retirement plan for one reaper tick.
///
/// `scale_up_in_flight` should be true when a scale-up request was sent to the
/// backend autoscaler within the last autoscaler cycle. This is intentionally
/// conservative: retirement is suppressed (and in-progress drains cancelled)
/// for a full cycle after *every* scale-up request, not just the tick it was
/// sent on, because the autoscaler may still be provisioning the capacity we
/// asked for — retiring workers then would contradict the demand we signaled.
pub(crate) fn plan_reap(
    policy: &DownscalePolicy,
    scale_up_in_flight: bool,
    workers: &[WorkerStatus],
) -> ReapPlan {
    let all_draining = || -> Vec<WorkerId> {
        workers
            .iter()
            .filter(|w| w.draining)
            .map(|w| w.worker_id.clone())
            .collect()
    };

    // Downscaling switched off (possibly mid-drain): nothing may stay hidden
    // from the scheduler.
    if !policy.enabled {
        return ReapPlan {
            undrain: all_draining(),
            ..Default::default()
        };
    }

    // Scale-up guard: demand is rising, cancel drains instead of fighting the
    // autoscaler over the same capacity.
    if scale_up_in_flight {
        return ReapPlan {
            undrain: all_draining(),
            ..Default::default()
        };
    }

    // Honor the min-survivor floor so we never collapse the warm pool between
    // queries in a session (#5683). Draining workers are still alive and count
    // toward the pool.
    let allowed_to_retire = workers.len().saturating_sub(policy.min_survivor_workers);

    let mut undrain = Vec::new();
    let mut release_candidates: Vec<(WorkerId, Duration)> = Vec::new();
    let mut drain_candidates: Vec<(WorkerId, Duration)> = Vec::new();

    for w in workers {
        let eligible =
            !w.is_head_node && w.idle_for.is_some_and(|idle| idle >= policy.idle_threshold);
        match (w.draining, eligible) {
            (true, true) => release_candidates.push((w.worker_id.clone(), w.idle_for.unwrap())),
            // Draining worker picked up work (the dispatch/drain race resolving
            // in favor of the task) or is otherwise no longer eligible: back in
            // service.
            (true, false) => undrain.push(w.worker_id.clone()),
            (false, true) => drain_candidates.push((w.worker_id.clone(), w.idle_for.unwrap())),
            (false, false) => {}
        }
    }

    // Longest-idle first.
    release_candidates.sort_by_key(|(_, idle)| std::cmp::Reverse(*idle));
    drain_candidates.sort_by_key(|(_, idle)| std::cmp::Reverse(*idle));

    // Release up to the floor allowance; any excess draining workers (e.g. the
    // floor was raised since they were marked) go back in service rather than
    // staying hidden forever.
    let mut release_iter = release_candidates.into_iter();
    let release: Vec<WorkerId> = release_iter
        .by_ref()
        .take(allowed_to_retire)
        .map(|(wid, _)| wid)
        .collect();
    undrain.extend(release_iter.map(|(wid, _)| wid));

    // Cap total hidden capacity (releases this tick + new drains) at the
    // allowance so draining never dips the visible pool below the floor.
    let remaining_allowance = allowed_to_retire.saturating_sub(release.len());
    let drain: Vec<WorkerId> = drain_candidates
        .into_iter()
        .take(remaining_allowance)
        .map(|(wid, _)| wid)
        .collect();

    ReapPlan {
        release,
        drain,
        undrain,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use super::*;

    fn policy(enabled: bool, floor: usize, idle_secs: u64) -> DownscalePolicy {
        DownscalePolicy {
            enabled,
            min_survivor_workers: floor,
            idle_threshold: Duration::from_secs(idle_secs),
        }
    }

    fn worker(id: &str, idle_secs: Option<u64>, draining: bool) -> WorkerStatus {
        WorkerStatus {
            worker_id: Arc::from(id),
            is_head_node: false,
            idle_for: idle_secs.map(Duration::from_secs),
            draining,
        }
    }

    fn ids(mut v: Vec<WorkerId>) -> Vec<String> {
        v.sort();
        v.into_iter().map(|w| w.to_string()).collect()
    }

    #[test]
    fn test_policy_from_lookup_defaults() {
        let policy = DownscalePolicy::from_lookup(|_| None);
        assert!(!policy.enabled);
        assert_eq!(policy.min_survivor_workers, DEFAULT_MIN_SURVIVOR_WORKERS);
        assert_eq!(
            policy.idle_threshold,
            Duration::from_secs(DEFAULT_IDLE_SECONDS)
        );
    }

    #[test]
    fn test_policy_from_lookup_parses_values() {
        let env: HashMap<&str, &str> = HashMap::from([
            (DOWNSCALE_ENABLED_ENV, "true"),
            (MIN_SURVIVOR_WORKERS_ENV, "3"),
            (DOWNSCALE_IDLE_SECONDS_ENV, "10"),
        ]);
        let policy = DownscalePolicy::from_lookup(|k| env.get(k).map(|v| v.to_string()));
        assert!(policy.enabled);
        assert_eq!(policy.min_survivor_workers, 3);
        assert_eq!(policy.idle_threshold, Duration::from_secs(10));
    }

    #[test]
    fn test_disabled_is_noop_and_undrains_everything() {
        // Flag flipped off mid-drain: draining workers must be restored so they
        // don't stay flagged as retiring forever.
        let plan = plan_reap(
            &policy(false, 1, 60),
            false,
            &[
                worker("w1", Some(120), true),
                worker("w2", Some(120), false),
            ],
        );
        assert!(plan.release.is_empty());
        assert!(plan.drain.is_empty());
        assert_eq!(ids(plan.undrain), vec!["w1"]);
    }

    #[test]
    fn test_scale_up_guard_cancels_drains_and_suppresses_retirement() {
        let plan = plan_reap(
            &policy(true, 0, 60),
            true,
            &[
                worker("w1", Some(120), true),
                worker("w2", Some(120), false),
            ],
        );
        assert!(plan.release.is_empty());
        assert!(plan.drain.is_empty());
        assert_eq!(ids(plan.undrain), vec!["w1"]);
    }

    #[test]
    fn test_two_phase_drain_then_release() {
        // Tick 1: idle-past-threshold worker is only marked as draining.
        let workers = [worker("w1", Some(120), false), worker("w2", Some(0), false)];
        let plan = plan_reap(&policy(true, 1, 60), false, &workers);
        assert!(plan.release.is_empty());
        assert_eq!(ids(plan.drain), vec!["w1"]);
        assert!(plan.undrain.is_empty());

        // Tick 2: still idle -> released.
        let workers = [worker("w1", Some(125), true), worker("w2", Some(5), false)];
        let plan = plan_reap(&policy(true, 1, 60), false, &workers);
        assert_eq!(ids(plan.release), vec!["w1"]);
        assert!(plan.drain.is_empty());
        assert!(plan.undrain.is_empty());
    }

    #[test]
    fn test_draining_worker_that_picked_up_tasks_is_undrained() {
        // The dispatch/drain race resolved in favor of the task: the worker is
        // busy again and must be put back in service, not released.
        let plan = plan_reap(&policy(true, 0, 60), false, &[worker("w1", None, true)]);
        assert!(plan.release.is_empty());
        assert!(plan.drain.is_empty());
        assert_eq!(ids(plan.undrain), vec!["w1"]);
    }

    #[test]
    fn test_idle_threshold_protects_recently_busy_workers() {
        let plan = plan_reap(
            &policy(true, 0, 60),
            false,
            &[worker("w1", Some(30), false)],
        );
        assert_eq!(plan, ReapPlan::default());
    }

    #[test]
    fn test_min_survivor_floor_is_never_violated() {
        // Three draining workers all idle past threshold, floor of 1: only two
        // may be released; the third goes back in service instead of staying
        // hidden below the floor.
        let plan = plan_reap(
            &policy(true, 1, 60),
            false,
            &[
                worker("w1", Some(300), true),
                worker("w2", Some(200), true),
                worker("w3", Some(100), true),
            ],
        );
        assert_eq!(ids(plan.release), vec!["w1", "w2"]);
        assert!(plan.drain.is_empty());
        assert_eq!(ids(plan.undrain), vec!["w3"]);
    }

    #[test]
    fn test_new_drains_capped_by_allowance_after_releases() {
        // Floor 1 with 4 workers: allowance is 3. Two draining workers are
        // released, so only one more may start draining this tick.
        let plan = plan_reap(
            &policy(true, 1, 60),
            false,
            &[
                worker("w1", Some(300), true),
                worker("w2", Some(200), true),
                worker("w3", Some(150), false),
                worker("w4", Some(100), false),
            ],
        );
        assert_eq!(ids(plan.release), vec!["w1", "w2"]);
        // Longest-idle non-draining candidate wins the remaining slot.
        assert_eq!(ids(plan.drain), vec!["w3"]);
        assert!(plan.undrain.is_empty());
    }

    #[test]
    fn test_head_node_is_never_drained_or_released() {
        let mut head = worker("head", Some(1000), false);
        head.is_head_node = true;
        let plan = plan_reap(&policy(true, 0, 60), false, &[head]);
        assert_eq!(plan, ReapPlan::default());
    }

    #[test]
    fn test_zero_floor_allows_draining_entire_pool() {
        let plan = plan_reap(
            &policy(true, 0, 60),
            false,
            &[
                worker("w1", Some(120), false),
                worker("w2", Some(90), false),
            ],
        );
        assert!(plan.release.is_empty());
        assert_eq!(ids(plan.drain), vec!["w1", "w2"]);
        assert!(plan.undrain.is_empty());
    }
}
