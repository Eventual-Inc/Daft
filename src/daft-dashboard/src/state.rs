use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use common_metrics::{NodeID, QueryID, QueryPlan, Stat};
use daft_recordbatch::RecordBatch;
use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::{broadcast, watch};
use uuid::Uuid;

use crate::engine::TaskStatsEntry;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) enum OperatorStatus {
    Pending,
    Executing,
    Finished,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NodeInfo {
    pub id: NodeID,
    pub name: String,
    pub node_type: Arc<str>,
    pub node_category: Arc<str>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OperatorInfo {
    pub status: OperatorStatus,
    pub node_info: NodeInfo,
    pub stats: HashMap<String, Stat>,
    #[serde(skip)]
    pub source_stats: HashMap<String, HashMap<String, Stat>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_sec: Option<f64>,
}

pub(crate) type OperatorInfos = HashMap<NodeID, OperatorInfo>;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status")]
pub(crate) enum TaskStatus {
    Pending,
    Running,
    Finished,
    Failed { message: Option<String> },
    Cancelled,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TaskInfo {
    pub task_id: u32,
    pub last_node_id: NodeID,
    pub node_ids: Vec<NodeID>,
    pub plan_fingerprint: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub status: TaskStatus,
    pub submit_sec: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Cumulative per-operator busy time for this task (sum of DURATION_KEY
    /// across operators, in microseconds). Set on task end, and refreshed
    /// mid-flight from `TaskStatsUpdate` events. Distinct from wall-clock
    /// elapsed time `end_sec - submit_sec`.
    pub cpu_us: u64,
    /// External rows read into the task (sum of `rows.in` only on local plan
    /// leaf nodes; internal flows between fused operators don't contribute).
    pub rows_in: u64,
    /// External rows emitted from the task (the local plan root's `rows.out`).
    pub rows_out: u64,
    /// External bytes read into the task (leaf nodes only).
    pub bytes_in: u64,
    /// External bytes emitted from the task (root only).
    pub bytes_out: u64,
    /// Wall-clock timestamp (sec since epoch) of the most recent mid-flight
    /// stats refresh. Used to drop stale out-of-order updates. Internal-only;
    /// not serialised to dashboard clients.
    #[serde(skip)]
    pub latest_stats_sec: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<crate::engine::TaskSourceArgs>,
}

/// Canonical group identity: the chain of distributed pipeline nodes that
/// contributed local plan nodes to the task, plus the local-plan shape
/// (`task.name`, e.g. `"InMemoryScan->Sample->Project"`). Two tasks with the
/// same `node_ids` and the same shape belong to the same group; different
/// shapes under the same `node_ids` (e.g. the sample / repartition / final
/// phases of a distributed Sort) get their own groups so the UI can show
/// per-phase counts and totals. The frontend overlays a parent section by
/// `node_ids` to keep sibling phases visually together.
///
/// `task.name` derives from `LocalPhysicalPlan::single_line_display()` which
/// uses operator variant names (`"Limit"`, not `"Limit(10)"`), so groups
/// don't split on parameter values like limit/partition counts.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TaskGroupKey {
    pub node_ids: Vec<NodeID>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TaskGroupSummary {
    pub last_node_id: NodeID,
    pub node_ids: Vec<NodeID>,
    pub name: String,
    pub task_count: u32,
    pub pending_count: u32,
    pub running_count: u32,
    pub finished_count: u32,
    pub failed_count: u32,
    pub cancelled_count: u32,
    pub total_cpu_us: u64,
    pub total_rows_in: u64,
    pub total_rows_out: u64,
    pub total_bytes_in: u64,
    pub total_bytes_out: u64,
    pub first_submit_sec: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_end_sec: Option<f64>,
    pub retained_task_count: u32,
}

/// Bounded task store that maintains per-group aggregate summaries and retains
/// only "interesting" individual tasks: active (in-flight), failed/cancelled,
/// and the top-K busy-time completed tasks per group.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct TaskStore {
    /// Aggregate summaries per `node_ids` group.
    pub groups: Vec<TaskGroupSummary>,
    /// Retained individual tasks (active, failed, top-K completed per group).
    pub tasks: HashMap<u32, TaskInfo>,

    /// Maps TaskGroupKey -> index into `groups` vec.
    #[serde(skip)]
    group_index: HashMap<TaskGroupKey, usize>,
    /// Per-group list of retained finished-successful task IDs.
    #[serde(skip)]
    finished_ids_by_group: HashMap<TaskGroupKey, Vec<u32>>,
    /// FIFO list of retained failed/cancelled task IDs.
    #[serde(skip)]
    failed_ids: VecDeque<u32>,
    #[serde(skip)]
    max_finished_per_group: usize,
    #[serde(skip)]
    max_retained_failed: usize,
}

impl Default for TaskStore {
    fn default() -> Self {
        Self::new(10, 100)
    }
}

impl TaskStore {
    pub fn new(max_finished_per_group: usize, max_retained_failed: usize) -> Self {
        Self {
            groups: Vec::new(),
            tasks: HashMap::new(),
            group_index: HashMap::new(),
            finished_ids_by_group: HashMap::new(),
            failed_ids: VecDeque::new(),
            max_finished_per_group,
            max_retained_failed,
        }
    }

    /// Returns the index into `self.groups` for the given key, creating the
    /// group if it doesn't exist yet. If the group already exists with an
    /// empty `node_ids` (e.g. when an end-before-submit fallback created it
    /// with `vec![last_node_id]`), upgrade its `node_ids` to the more
    /// informative chain when one is supplied.
    fn ensure_group(
        &mut self,
        last_node_id: NodeID,
        node_ids: &[NodeID],
        name: &str,
        initial_sec: f64,
    ) -> usize {
        // Resolve the canonical key. End-before-submit may arrive with empty
        // node_ids; fall back to a single-element chain so submit and end
        // converge to the same group.
        let key_node_ids: Vec<NodeID> = if node_ids.is_empty() {
            vec![last_node_id]
        } else {
            node_ids.to_vec()
        };
        let key = TaskGroupKey {
            node_ids: key_node_ids,
            name: name.to_string(),
        };
        let groups = &mut self.groups;
        *self.group_index.entry(key).or_insert_with_key(|key| {
            let idx = groups.len();
            groups.push(TaskGroupSummary {
                last_node_id,
                node_ids: key.node_ids.clone(),
                name: key.name.clone(),
                task_count: 0,
                pending_count: 0,
                running_count: 0,
                finished_count: 0,
                failed_count: 0,
                cancelled_count: 0,
                total_cpu_us: 0,
                total_rows_in: 0,
                total_rows_out: 0,
                total_bytes_in: 0,
                total_bytes_out: 0,
                first_submit_sec: initial_sec,
                last_end_sec: None,
                retained_task_count: 0,
            });
            idx
        })
    }

    /// Derive the group key for a task from its full `node_ids` chain plus
    /// its local-plan shape. Falls back to a single-element chain if
    /// `node_ids` is empty, and to `Node {last_node_id}` if `task.name` is
    /// missing (matching the display-name fallback in `submit_task` /
    /// `end_task`).
    fn group_key_for_task(task: &TaskInfo) -> TaskGroupKey {
        let node_ids = if task.node_ids.is_empty() {
            vec![task.last_node_id]
        } else {
            task.node_ids.clone()
        };
        let name = task
            .name
            .clone()
            .unwrap_or_else(|| format!("Node {}", task.last_node_id));
        TaskGroupKey { node_ids, name }
    }

    /// Record a task submission. The individual task is always retained while
    /// active (callers inspect active tasks for debugging stuck queries).
    pub fn submit_task(&mut self, args: crate::engine::TaskSubmitArgs) {
        let crate::engine::TaskSubmitArgs {
            submit_sec,
            task_id,
            last_node_id,
            node_ids,
            plan_fingerprint,
            name,
            sources,
        } = args;
        // The group key is `(node_ids, display_name)`, so two tasks under the
        // same distributed-plan chain that render with different local-plan
        // shapes (e.g. Sort's sample / repartition / final phases) split into
        // separate groups and get their own per-shape totals.
        let display_name = name
            .clone()
            .unwrap_or_else(|| format!("Node {last_node_id}"));
        let is_new = !self.tasks.contains_key(&task_id);
        let gi = self.ensure_group(last_node_id, &node_ids, &display_name, submit_sec);
        let group = &mut self.groups[gi];

        if is_new {
            group.task_count += 1;
            group.pending_count += 1;
            group.retained_task_count += 1;
            if submit_sec < group.first_submit_sec {
                group.first_submit_sec = submit_sec;
            }
        }

        let entry = self.tasks.entry(task_id).or_insert_with(|| TaskInfo {
            task_id,
            last_node_id,
            node_ids: node_ids.clone(),
            plan_fingerprint,
            name: name.clone(),
            status: TaskStatus::Pending,
            submit_sec,
            start_sec: None,
            end_sec: None,
            worker_id: None,
            cpu_us: 0,
            rows_in: 0,
            rows_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            latest_stats_sec: None,
            sources: Vec::new(),
        });

        // If a prior submit is replayed, keep the earliest submit_sec and sync
        // identifying fields.
        entry.last_node_id = last_node_id;
        entry.node_ids = node_ids;
        entry.plan_fingerprint = plan_fingerprint;
        if name.is_some() {
            entry.name = name;
        }
        if submit_sec < entry.submit_sec {
            entry.submit_sec = submit_sec;
        }
        if !sources.is_empty() {
            entry.sources = sources;
        }
    }

    /// Record a mid-execution scalar progress snapshot for an in-flight task.
    /// No-op if the task has reached a terminal status (snapshot is stale) or
    /// if this snapshot is older than one already applied.
    ///
    /// Updates the individual task's totals only. Group summaries are
    /// updated once at `end_task`. Mirroring mid-flight updates into group
    /// totals would seem appealing for a smoother dashboard but turned out
    /// to be brittle: the worker-emitted totals aren't strictly monotonic
    /// (e.g. cpu_us can drop between ticks if a blocking operator
    /// deactivates and stops contributing to the snapshot), and propagating
    /// those non-monotonic deltas into the group summary corrupts the
    /// running sum.
    ///
    /// Naming caveat: `cpu_us` here is **busy time** — cumulative wall-clock
    /// time spent inside `op.execute().await` across the task's local
    /// pipeline operators (the runtime stats layer stores it under
    /// `DURATION_KEY`). It is **not** elapsed wall-clock time since task
    /// submit (`end_sec - submit_sec`) that the frontend sometimes labels
    /// "duration". Two tasks with the same wall-clock duration can have very
    /// different busy times.
    pub fn update_task_stats(&mut self, entry: TaskStatsEntry, timestamp_sec: f64) {
        let Some(task) = self.tasks.get_mut(&entry.task_id) else {
            return;
        };
        // Stats arrive between submit and end — accept Pending and Running.
        // Drop in any terminal state.
        if !matches!(task.status, TaskStatus::Pending | TaskStatus::Running) {
            return;
        }
        if let Some(prev) = task.latest_stats_sec
            && prev > timestamp_sec
        {
            return;
        }
        task.cpu_us = entry.totals.cpu_us;
        task.rows_in = entry.totals.rows_in;
        task.rows_out = entry.totals.rows_out;
        task.bytes_in = entry.totals.bytes_in;
        task.bytes_out = entry.totals.bytes_out;
        task.latest_stats_sec = Some(timestamp_sec);
    }

    /// Record a task being scheduled to a worker (transitioning from Pending
    /// to Running). The dashboard treats "scheduled" as the running boundary
    /// even though true execution starts slightly later on the worker. If
    /// the submit event wasn't seen yet, the task is created here; group
    /// counters stay consistent because we only adjust pending/running on
    /// transitions.
    pub fn task_scheduled(
        &mut self,
        task_id: u32,
        last_node_id: NodeID,
        node_ids: Vec<NodeID>,
        plan_fingerprint: u32,
        worker_id: Option<String>,
        scheduled_sec: f64,
    ) {
        let display_name = self
            .tasks
            .get(&task_id)
            .and_then(|t| t.name.clone())
            .unwrap_or_else(|| format!("Node {last_node_id}"));

        let key_node_ids: Vec<NodeID> = if node_ids.is_empty() {
            self.tasks
                .get(&task_id)
                .map(|t| t.node_ids.clone())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| vec![last_node_id])
        } else {
            node_ids.clone()
        };

        let was_submitted = self.tasks.contains_key(&task_id);
        let was_pending = was_submitted
            && matches!(
                self.tasks.get(&task_id).map(|t| &t.status),
                Some(TaskStatus::Pending)
            );

        let gi = self.ensure_group(last_node_id, &key_node_ids, &display_name, scheduled_sec);
        let group = &mut self.groups[gi];

        if !was_submitted {
            // Schedule arrived before submit; treat as a new task that
            // bypassed the pending state. Increment task_count and
            // running_count.
            group.task_count += 1;
            group.running_count += 1;
            group.retained_task_count += 1;
        } else if was_pending {
            // Normal pending -> running transition.
            group.pending_count = group.pending_count.saturating_sub(1);
            group.running_count += 1;
        }
        // If already running/finished/etc, leave counters alone (idempotent).

        let task = self.tasks.entry(task_id).or_insert_with(|| TaskInfo {
            task_id,
            last_node_id,
            node_ids: node_ids.clone(),
            plan_fingerprint,
            name: None,
            status: TaskStatus::Pending,
            submit_sec: scheduled_sec, // no submit seen; use schedule time
            start_sec: None,
            end_sec: None,
            worker_id: None,
            cpu_us: 0,
            rows_in: 0,
            rows_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            latest_stats_sec: None,
            sources: Vec::new(),
        });

        task.last_node_id = last_node_id;
        if !node_ids.is_empty() {
            task.node_ids = node_ids;
        }
        task.plan_fingerprint = plan_fingerprint;

        // Only transition to Running if the task is currently Pending; don't
        // overwrite a terminal status if a schedule arrives after end.
        if matches!(task.status, TaskStatus::Pending) {
            task.status = TaskStatus::Running;
        }

        // `start_sec` is "best-known start time": the schedule time stands in
        // for the actual start until worker-side TaskStart wiring lands. Keep
        // the earliest signal we've seen.
        match task.start_sec {
            Some(prev) if prev <= scheduled_sec => {}
            _ => task.start_sec = Some(scheduled_sec),
        }
        if worker_id.is_some() {
            task.worker_id = worker_id;
        }
    }

    /// Record a task completion. Updates group summary and applies retention:
    /// failed/cancelled tasks are always kept (up to a global cap); successful
    /// tasks are kept if they are among the top-K by busy time per group.
    #[allow(clippy::too_many_arguments)]
    pub fn end_task(
        &mut self,
        task_id: u32,
        last_node_id: NodeID,
        node_ids: Vec<NodeID>,
        plan_fingerprint: u32,
        worker_id: Option<String>,
        status: TaskStatus,
        end_sec: f64,
        cpu_us: u64,
        rows_in: u64,
        rows_out: u64,
        bytes_in: u64,
        bytes_out: u64,
    ) {
        // Determine whether this task was previously submitted (exists in tasks).
        let was_submitted = self.tasks.contains_key(&task_id);
        let prev_status = self.tasks.get(&task_id).map(|t| t.status.clone());
        let was_pending = matches!(prev_status, Some(TaskStatus::Pending));
        let was_running = matches!(prev_status, Some(TaskStatus::Running));

        // Display label for the (possibly new) group. The actual group key
        // is `node_ids`; this is just what the UI shows.
        let display_name = self
            .tasks
            .get(&task_id)
            .and_then(|t| t.name.clone())
            .unwrap_or_else(|| format!("Node {last_node_id}"));

        // Resolve to the same key shape `ensure_group` will use, so retention
        // bookkeeping below addresses the right group.
        let key_node_ids: Vec<NodeID> = if node_ids.is_empty() {
            self.tasks
                .get(&task_id)
                .map(|t| t.node_ids.clone())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| vec![last_node_id])
        } else {
            node_ids.clone()
        };
        let key = TaskGroupKey {
            node_ids: key_node_ids.clone(),
            name: display_name.clone(),
        };

        // Ensure group exists and update summary.
        let gi = self.ensure_group(last_node_id, &key_node_ids, &display_name, end_sec);
        let group = &mut self.groups[gi];

        if !was_submitted {
            group.task_count += 1;
        }
        if was_pending {
            group.pending_count = group.pending_count.saturating_sub(1);
        }
        if was_running {
            group.running_count = group.running_count.saturating_sub(1);
        }
        match &status {
            TaskStatus::Finished => group.finished_count += 1,
            TaskStatus::Failed { .. } => group.failed_count += 1,
            TaskStatus::Cancelled => group.cancelled_count += 1,
            TaskStatus::Pending | TaskStatus::Running => {}
        }

        // Mid-flight `update_task_stats` writes only into the individual
        // TaskInfo, so end_task adds the full task total to the group sum.
        group.total_cpu_us += cpu_us;
        group.total_rows_in += rows_in;
        group.total_rows_out += rows_out;
        group.total_bytes_in += bytes_in;
        group.total_bytes_out += bytes_out;
        match group.last_end_sec {
            Some(prev) if prev >= end_sec => {}
            _ => group.last_end_sec = Some(end_sec),
        }

        if !was_submitted {
            group.retained_task_count += 1;
        }

        // Upsert individual task.
        let task = self.tasks.entry(task_id).or_insert_with(|| TaskInfo {
            task_id,
            last_node_id,
            node_ids: node_ids.clone(),
            plan_fingerprint,
            name: None,
            status: TaskStatus::Pending,
            submit_sec: end_sec, // no submit seen; use end time
            start_sec: None,
            end_sec: None,
            worker_id: None,
            cpu_us: 0,
            rows_in: 0,
            rows_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            latest_stats_sec: None,
            sources: Vec::new(),
        });

        task.last_node_id = last_node_id;
        if !node_ids.is_empty() {
            task.node_ids = node_ids;
        }
        task.plan_fingerprint = plan_fingerprint;
        task.status = status.clone();
        task.end_sec = Some(end_sec);
        task.worker_id = worker_id;
        task.cpu_us = cpu_us;
        task.rows_in = rows_in;
        task.rows_out = rows_out;
        task.bytes_in = bytes_in;
        task.bytes_out = bytes_out;

        // Apply retention policy.
        match &status {
            TaskStatus::Finished => {
                self.retain_finished_task(task_id, &key);
            }
            TaskStatus::Failed { .. } | TaskStatus::Cancelled => {
                self.retain_failed_task(task_id);
            }
            TaskStatus::Pending | TaskStatus::Running => {}
        }
    }

    /// Keep top-K finished tasks per group, ranked by busy time.
    fn retain_finished_task(&mut self, task_id: u32, key: &TaskGroupKey) {
        self.finished_ids_by_group
            .entry(key.clone())
            .or_default()
            .push(task_id);

        // Check whether eviction is needed; clone the (small, K~10) id list to
        // break the borrow on self.finished_ids_by_group so we can read
        // self.tasks freely.
        let ids_snapshot = match self.finished_ids_by_group.get(key) {
            Some(ids) if ids.len() > self.max_finished_per_group => ids.clone(),
            _ => return,
        };

        // Find the task with the least busy time to evict.
        let evict = ids_snapshot
            .iter()
            .enumerate()
            .min_by_key(|(_, id)| self.tasks.get(id).map(|t| t.cpu_us).unwrap_or(0))
            .map(|(idx, _)| idx);

        if let Some(evict_idx) = evict {
            let evicted_id = ids_snapshot[evict_idx];
            if let Some(ids) = self.finished_ids_by_group.get_mut(key) {
                ids.swap_remove(evict_idx);
            }
            self.tasks.remove(&evicted_id);
            if let Some(&gi) = self.group_index.get(key) {
                self.groups[gi].retained_task_count =
                    self.groups[gi].retained_task_count.saturating_sub(1);
            }
        }
    }

    /// Keep failed/cancelled tasks up to a global FIFO cap.
    fn retain_failed_task(&mut self, task_id: u32) {
        self.failed_ids.push_back(task_id);

        if self.failed_ids.len() > self.max_retained_failed
            && let Some(evicted_id) = self.failed_ids.pop_front()
            && let Some(evicted) = self.tasks.remove(&evicted_id)
        {
            let evicted_key = Self::group_key_for_task(&evicted);
            if let Some(gi) = self.group_index.get(&evicted_key) {
                self.groups[*gi].retained_task_count =
                    self.groups[*gi].retained_task_count.saturating_sub(1);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PlanInfo {
    pub plan_start_sec: f64,
    pub plan_end_sec: f64,
    pub optimized_plan: QueryPlan,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExecInfo {
    pub exec_start_sec: f64,
    pub physical_plan: QueryPlan,
    pub operators: OperatorInfos,
    /// Bounded task store for Flotilla queries. Maintains per-group aggregate
    /// summaries and retains only interesting individual tasks (active, failed,
    /// top-K by busy time). Empty for Swordfish (local) queries.
    #[serde(default)]
    pub task_store: TaskStore,
    // TODO: Logs
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status")]
pub(crate) enum QueryStatus {
    Pending {
        start_sec: f64,
    },
    Optimizing {
        plan_start_sec: f64,
    },
    Setup,
    Executing {
        exec_start_sec: f64,
    },
    Finalizing,
    Finished {
        duration_sec: f64,
    },
    Canceled {
        duration_sec: f64,
        message: Option<String>,
    },
    Failed {
        duration_sec: f64,
        message: Option<String>,
    },
    Dead {
        duration_sec: f64,
    },
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QuerySummary {
    pub id: QueryID,
    pub start_sec: f64,
    pub status: QueryStatus,
    pub runner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ray_dashboard_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub python_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daft_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ray_version: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
#[serde(tag = "status")]
pub(crate) enum QueryState {
    Pending,
    Optimizing {
        plan_start_sec: f64,
    },
    Setup {
        plan_info: PlanInfo,
        #[serde(skip)]
        pending_source_stats: HashMap<String, HashMap<NodeID, HashMap<String, Stat>>>,
    },
    Executing {
        plan_info: PlanInfo,
        exec_info: ExecInfo,
    },
    Finalizing {
        plan_info: PlanInfo,
        exec_info: ExecInfo,
        exec_end_sec: f64,
    },
    Finished {
        plan_info: PlanInfo,
        exec_info: ExecInfo,
        exec_end_sec: f64,
        end_sec: f64,
    },
    Canceled {
        plan_info: Option<PlanInfo>,
        exec_info: Option<ExecInfo>,
        end_sec: f64,
        message: Option<String>,
    },
    Failed {
        plan_info: Option<PlanInfo>,
        exec_info: Option<ExecInfo>,
        end_sec: f64,
        message: Option<String>,
    },
    Dead {
        plan_info: Option<PlanInfo>,
        exec_info: Option<ExecInfo>,
        marked_dead_sec: f64,
    },
}

impl QueryState {
    pub(crate) fn is_active(&self) -> bool {
        // `Finalizing` is included so a query awaiting `query_end` is not
        // mistaken for terminal: eviction would otherwise drop it before its
        // final outcome is recorded, and the dead-query reaper would skip it.
        matches!(
            self,
            Self::Pending
                | Self::Optimizing { .. }
                | Self::Setup { .. }
                | Self::Executing { .. }
                | Self::Finalizing { .. }
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QueryInfo {
    pub id: QueryID,
    pub start_sec: f64,
    pub last_heartbeat_sec: f64,
    pub unoptimized_plan: QueryPlan,
    pub runner: String,
    pub ray_dashboard_url: Option<String>,
    pub entrypoint: Option<String>,
    pub python_version: Option<String>,
    pub daft_version: Option<String>,
    pub ray_version: Option<String>,
    pub state: QueryState,
}

impl QueryInfo {
    pub fn is_active(&self) -> bool {
        self.state.is_active()
    }

    pub fn status(&self) -> QueryStatus {
        match &self.state {
            QueryState::Pending => QueryStatus::Pending {
                start_sec: self.start_sec,
            },
            QueryState::Optimizing { plan_start_sec } => QueryStatus::Optimizing {
                plan_start_sec: *plan_start_sec,
            },
            // Pending between optimizing and execution
            QueryState::Setup { .. } => QueryStatus::Setup,
            QueryState::Executing { exec_info, .. } => QueryStatus::Executing {
                exec_start_sec: exec_info.exec_start_sec,
            },
            // Finalizing may take longer so just in case
            QueryState::Finalizing { .. } => QueryStatus::Finalizing,
            QueryState::Finished { end_sec, .. } => QueryStatus::Finished {
                duration_sec: end_sec - self.start_sec,
            },
            QueryState::Canceled {
                end_sec, message, ..
            } => QueryStatus::Canceled {
                duration_sec: end_sec - self.start_sec,
                message: message.clone(),
            },
            QueryState::Failed {
                end_sec, message, ..
            } => QueryStatus::Failed {
                duration_sec: end_sec - self.start_sec,
                message: message.clone(),
            },
            QueryState::Dead {
                marked_dead_sec, ..
            } => QueryStatus::Dead {
                duration_sec: marked_dead_sec - self.start_sec,
            },
        }
    }

    pub fn summarize(&self) -> QuerySummary {
        QuerySummary {
            id: self.id.clone(),
            start_sec: self.start_sec,
            status: self.status(),
            runner: self.runner.clone(),
            ray_dashboard_url: self.ray_dashboard_url.clone(),
            entrypoint: self.entrypoint.clone(),
            python_version: self.python_version.clone(),
            daft_version: self.daft_version.clone(),
            ray_version: self.ray_version.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DashboardState {
    // Mapping from query id to query info
    pub queries: DashMap<QueryID, QueryInfo>,
    pub dataframe_previews: DashMap<String, RecordBatch>,
    pub clients: broadcast::Sender<(usize, QuerySummary)>,
    pub query_clients: DashMap<QueryID, (watch::Sender<QueryInfo>, watch::Sender<OperatorInfos>)>,
    pub event_counter: AtomicUsize,
}

impl DashboardState {
    pub fn new() -> Self {
        Self {
            queries: Default::default(),
            dataframe_previews: Default::default(),
            // TODO: Ideally this should never drop events, we need an unbounded broadcast channel
            clients: broadcast::Sender::new(256),
            query_clients: Default::default(),
            event_counter: AtomicUsize::new(0),
        }
    }

    pub fn register_dataframe_preview(&self, record_batch: RecordBatch) -> String {
        let id = Uuid::new_v4().to_string();
        self.dataframe_previews.insert(id.clone(), record_batch);
        id
    }

    pub fn get_dataframe_preview(&self, id: &str) -> Option<RecordBatch> {
        self.dataframe_previews.get(id).map(|r| r.value().clone())
    }

    // -------------------- Updating Queries -------------------- //

    pub fn ping_clients_on_query_update(&self, query_info: &QueryInfo) {
        let id = self.event_counter.fetch_add(1, Ordering::SeqCst);

        if let Some(query_client) = self.query_clients.get(&query_info.id) {
            let _ = query_client.0.send(query_info.clone());
        }

        let _ = self.clients.send((id, query_info.summarize()));
    }

    pub fn ping_clients_on_operator_update(&self, query_info: &QueryInfo) {
        let query_id = &query_info.id;
        if let Some(query_client) = self.query_clients.get(query_id) {
            match &query_info.state {
                QueryState::Executing { exec_info, .. }
                | QueryState::Finalizing { exec_info, .. }
                | QueryState::Finished { exec_info, .. }
                | QueryState::Failed {
                    exec_info: Some(exec_info),
                    ..
                }
                | QueryState::Canceled {
                    exec_info: Some(exec_info),
                    ..
                }
                | QueryState::Dead {
                    exec_info: Some(exec_info),
                    ..
                } => {
                    let operator_infos = exec_info.operators.clone();
                    let _ = query_client.1.send(operator_infos);
                }
                _ => {
                    tracing::warn!(
                        "Query `{}` is not in an executing state (current: {:?}), skipping operator update",
                        query_id,
                        query_info.state
                    );
                }
            }
        }
    }
}

// Global shared dashboard state for this process.
pub static GLOBAL_DASHBOARD_STATE: LazyLock<Arc<DashboardState>> =
    LazyLock::new(|| Arc::new(DashboardState::new()));

#[cfg(test)]
mod task_store_tests {
    use super::*;
    use crate::engine::{TaskStatsEntry, TaskSubmitArgs, TaskTotals};

    fn finished() -> TaskStatus {
        TaskStatus::Finished
    }

    fn stats_entry(task_id: u32, cpu_us: u64) -> TaskStatsEntry {
        TaskStatsEntry {
            task_id,
            totals: TaskTotals {
                cpu_us,
                ..TaskTotals::default()
            },
        }
    }

    fn submit(
        task_id: u32,
        last_node_id: NodeID,
        node_ids: Vec<NodeID>,
        name: &str,
        submit_sec: f64,
    ) -> TaskSubmitArgs {
        TaskSubmitArgs {
            submit_sec,
            task_id,
            last_node_id,
            node_ids,
            plan_fingerprint: 0,
            name: Some(name.to_string()),
            sources: vec![],
        }
    }

    /// Two tasks with identical `node_ids` and identical `name` collapse into
    /// one group. `task.name` derives from `LocalPhysicalPlan::name()`
    /// (operator variant only — not parameter values), so two `Limit` tasks
    /// with different limit values render with the same name and merge.
    #[test]
    fn same_node_ids_same_name_collapse_to_one_group() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 7, vec![3, 5, 7], "Limit", 0.0));
        store.submit_task(submit(2, 7, vec![3, 5, 7], "Limit", 0.0));

        assert_eq!(
            store.groups.len(),
            1,
            "expected one group, got {:?}",
            store.groups
        );
        assert_eq!(store.groups[0].task_count, 2);
        assert_eq!(store.groups[0].node_ids, vec![3, 5, 7]);
    }

    /// Two tasks under the same distributed-plan chain that render with
    /// different local-plan shapes split into separate groups. Concrete case:
    /// a distributed Sort dispatches three phases (sample / repartition /
    /// final) all with the same `node_ids = [sort_node_id]`, but their local
    /// plans render as `InMemoryScan->Sample->Project`,
    /// `InMemoryScan->RepartitionWrite`, and `InMemoryScan->Sort`. Each phase
    /// gets its own group so per-phase counts and totals don't get rolled up
    /// together.
    #[test]
    fn same_node_ids_different_names_split_into_separate_groups() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 7, vec![7], "InMemoryScan->Sample->Project", 0.0));
        store.submit_task(submit(2, 7, vec![7], "InMemoryScan->RepartitionWrite", 0.0));
        store.submit_task(submit(3, 7, vec![7], "InMemoryScan->Sort", 0.0));

        assert_eq!(store.groups.len(), 3);
        for g in &store.groups {
            assert_eq!(g.node_ids, vec![7]);
            assert_eq!(g.task_count, 1);
        }
    }

    /// Two tasks with the same `name` but different `node_ids` should land in
    /// distinct groups (chain identity differs).
    #[test]
    fn different_node_ids_same_name_split_into_two_groups() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 5, vec![3, 5], "Project", 0.0));
        store.submit_task(submit(2, 9, vec![7, 9], "Project", 0.0));

        assert_eq!(store.groups.len(), 2);
    }

    /// Mid-flight stats update writes cpu_us through to the task and stamps
    /// latest_stats_sec, but intentionally does NOT touch group totals — group
    /// summaries are only updated at task end. See `update_task_stats` for the
    /// rationale (worker-emitted task totals aren't strictly monotonic, so
    /// streaming deltas into the group sum is brittle).
    #[test]
    fn update_task_stats_writes_task_only_not_group() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 7, vec![3, 5, 7], "Filter", 0.0));

        store.update_task_stats(stats_entry(1, 300), 1.0);

        let task = store.tasks.get(&1).expect("task retained");
        assert_eq!(task.cpu_us, 300);
        assert_eq!(task.latest_stats_sec, Some(1.0));
        // worker_id is set by scheduler-side TaskStart/TaskEnd, not by us.
        assert!(task.worker_id.is_none());
        assert_eq!(store.groups.len(), 1);
        // Group total stays 0 until end_task.
        assert_eq!(store.groups[0].total_cpu_us, 0);
    }

    /// Successive updates overwrite the task's cpu_us (latest wins). Worker
    /// snapshots can be non-monotonic when blocking operators deactivate, so
    /// we don't try to enforce monotonicity here.
    #[test]
    fn update_task_stats_overwrites_with_latest() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 7, vec![7], "Filter", 0.0));

        store.update_task_stats(stats_entry(1, 200), 1.0);
        store.update_task_stats(stats_entry(1, 500), 2.0);

        assert_eq!(store.tasks.get(&1).unwrap().cpu_us, 500);
        assert_eq!(store.tasks.get(&1).unwrap().latest_stats_sec, Some(2.0));
    }

    /// An out-of-order update older than the last seen timestamp is dropped
    /// (cpu_us and latest_stats_sec stay at the newer value).
    #[test]
    fn update_task_stats_drops_stale_timestamp() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 7, vec![7], "Filter", 0.0));

        store.update_task_stats(stats_entry(1, 400), 5.0);
        store.update_task_stats(stats_entry(1, 999), 2.0); // stale

        let task = store.tasks.get(&1).unwrap();
        assert_eq!(task.cpu_us, 400);
        assert_eq!(task.latest_stats_sec, Some(5.0));
    }

    /// After end_task the task is no longer Pending; a late stats update must
    /// be ignored so it can't overwrite the final cpu_us.
    #[test]
    fn update_task_stats_after_end_is_noop() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 7, vec![7], "Filter", 0.0));
        store.end_task(1, 7, vec![7], 0, None, finished(), 1.0, 600, 0, 0, 0, 0);

        store.update_task_stats(stats_entry(1, 9_999), 2.0);

        assert_eq!(store.tasks.get(&1).unwrap().cpu_us, 600);
        assert_eq!(store.groups[0].total_cpu_us, 600);
    }

    /// end_task adds the full task total to the parent group's running sum.
    /// Mid-flight `update_task_stats` calls don't contribute to the group, so
    /// there is no risk of double-counting.
    #[test]
    fn end_task_adds_full_total_to_group() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 7, vec![7], "Filter", 0.0));

        // Mid-flight stats land on the task but not the group.
        store.update_task_stats(stats_entry(1, 300), 1.0);
        assert_eq!(store.groups[0].total_cpu_us, 0);

        store.end_task(1, 7, vec![7], 0, None, finished(), 2.0, 500, 0, 0, 0, 0);

        assert_eq!(store.tasks.get(&1).unwrap().cpu_us, 500);
        assert_eq!(store.groups[0].total_cpu_us, 500);
    }

    /// Stats updates for an unknown task_id (never submitted) are dropped
    /// silently — they may arrive during the submit/exec_start race.
    #[test]
    fn update_task_stats_unknown_task_is_noop() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 7, vec![7], "Filter", 0.0));

        store.update_task_stats(stats_entry(999, 1_000), 1.0);

        assert!(!store.tasks.contains_key(&999));
    }
    /// End-before-submit with empty node_ids creates a fallback group keyed
    /// by `(vec![last_node_id], "Node {last_node_id}")` because the local-plan
    /// name isn't known until submit arrives. When submit eventually arrives
    /// with the real name, the task isn't double-counted (it's already in
    /// `tasks` so the new group's counters are not incremented), but the
    /// fallback group remains as an empty-counts ghost row. End-before-submit
    /// only happens when network reordering delivers the end event before
    /// the submit; in steady state this is rare. A future change could
    /// migrate the fallback group's counts into the real group when submit
    /// arrives, but that's heavier than the bug warrants today.
    #[test]
    fn end_before_submit_with_empty_node_ids_creates_fallback_group() {
        let mut store = TaskStore::default();

        store.end_task(
            42,
            7,
            vec![],
            0,
            Some("worker-1".to_string()),
            finished(),
            1.0,
            500,
            0,
            0,
            0,
            0,
        );
        // Submit arrives later with the same single-node chain.
        store.submit_task(submit(42, 7, vec![7], "Filter", 0.5));

        // Two groups: the fallback `(vec![7], "Node 7")` from end_task, and
        // the real `(vec![7], "Filter")` from submit_task.
        assert_eq!(store.groups.len(), 2);
        let fallback = store
            .groups
            .iter()
            .find(|g| g.name == "Node 7")
            .expect("fallback group present");
        assert_eq!(fallback.task_count, 1);
        assert_eq!(fallback.finished_count, 1);
        let real = store
            .groups
            .iter()
            .find(|g| g.name == "Filter")
            .expect("real group present");
        // Real group exists but contributes zero counts: the task already
        // ended so submit didn't increment further.
        assert_eq!(real.task_count, 0);
    }

    /// submit -> scheduled -> end transitions pending/running/finished counts
    /// correctly, and durations are computed from start_sec rather than submit_sec.
    #[test]
    fn submit_then_scheduled_then_end_transitions_counts() {
        let mut store = TaskStore::default();
        store.submit_task(submit(1, 7, vec![7], "Filter", 0.0));
        {
            let g = &store.groups[0];
            assert_eq!(g.pending_count, 1);
            assert_eq!(g.running_count, 0);
        }

        store.task_scheduled(1, 7, vec![7], 0, Some("worker-1".to_string()), 0.5);
        {
            let g = &store.groups[0];
            assert_eq!(g.pending_count, 0);
            assert_eq!(g.running_count, 1);
            let t = &store.tasks[&1];
            assert!(matches!(t.status, TaskStatus::Running));
            assert_eq!(t.start_sec, Some(0.5));
        }

        store.end_task(
            1,
            7,
            vec![7],
            0,
            Some("worker-1".to_string()),
            TaskStatus::Finished,
            2.0,
            123,
            0,
            0,
            0,
            0,
        );
        {
            let g = &store.groups[0];
            assert_eq!(g.pending_count, 0);
            assert_eq!(g.running_count, 0);
            assert_eq!(g.finished_count, 1);
        }
    }

    /// A schedule event arriving before submit should still credit task_count
    /// and running_count; subsequent submit must not double-count.
    #[test]
    fn scheduled_before_submit_does_not_double_count() {
        let mut store = TaskStore::default();
        store.task_scheduled(7, 3, vec![3], 0, Some("worker-2".to_string()), 1.0);
        {
            let g = &store.groups[0];
            assert_eq!(g.task_count, 1);
            assert_eq!(g.running_count, 1);
            assert_eq!(g.pending_count, 0);
        }

        store.submit_task(submit(7, 3, vec![3], "Project", 0.5));
        {
            let g = &store.groups[0];
            // Submit found an existing task; counters unchanged.
            assert_eq!(g.task_count, 1);
            assert_eq!(g.running_count, 1);
            assert_eq!(g.pending_count, 0);
        }
    }

    /// Sources passed to `submit_task` should be retained on the per-task
    /// record so the dashboard UI can render them.
    #[test]
    fn submit_task_retains_sources() {
        use crate::engine::{PhysicalScanSourceArgs, TaskSourceArgs};

        let mut store = TaskStore::default();
        let source = TaskSourceArgs::PhysicalScan(PhysicalScanSourceArgs {
            source_id: 0,
            scan_tasks: 1,
            paths: vec!["s3://bucket/file.parquet".to_string()],
            storage_bytes: Some(1024),
            estimated_memory_bytes: Some(4096),
        });
        let mut args = submit(1, 7, vec![3, 5, 7], "ScanTaskSource->Project", 0.0);
        args.sources = vec![source];
        store.submit_task(args);

        let task = store.tasks.get(&1).expect("task should be retained");
        assert_eq!(task.sources.len(), 1);
        match &task.sources[0] {
            TaskSourceArgs::PhysicalScan(p) => {
                assert_eq!(p.paths, vec!["s3://bucket/file.parquet".to_string()]);
            }
            _ => panic!("expected PhysicalScan source"),
        }
    }
}
