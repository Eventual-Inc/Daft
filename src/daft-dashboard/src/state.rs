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
    /// stats refresh. Used to drop stale out-of-order updates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_stats_sec: Option<f64>,
}

/// Canonical group identity: the full chain of distributed pipeline nodes that
/// contributed local plan nodes to the task. Two tasks with identical chains
/// belong to the same group, regardless of how `task.name` was rendered or
/// what parameter values varied (limit values, repartition counts, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TaskGroupKey {
    pub node_ids: Vec<NodeID>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TaskGroupSummary {
    pub last_node_id: NodeID,
    pub node_ids: Vec<NodeID>,
    pub name: String,
    pub task_count: u32,
    pub pending_count: u32,
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
        };
        let groups = &mut self.groups;
        *self.group_index.entry(key).or_insert_with_key(|key| {
            let idx = groups.len();
            groups.push(TaskGroupSummary {
                last_node_id,
                node_ids: key.node_ids.clone(),
                name: name.to_string(),
                task_count: 0,
                pending_count: 0,
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

    /// Derive the group key for a task from its full `node_ids` chain. Falls
    /// back to a single-element chain if `node_ids` is empty.
    fn group_key_for_task(task: &TaskInfo) -> TaskGroupKey {
        let node_ids = if task.node_ids.is_empty() {
            vec![task.last_node_id]
        } else {
            task.node_ids.clone()
        };
        TaskGroupKey { node_ids }
    }

    /// Record a task submission. The individual task is always retained while
    /// active (callers inspect active tasks for debugging stuck queries).
    pub fn submit_task(
        &mut self,
        task_id: u32,
        last_node_id: NodeID,
        node_ids: Vec<NodeID>,
        plan_fingerprint: u32,
        name: Option<String>,
        submit_sec: f64,
    ) {
        // Display label only; the group key is `node_ids`.
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
            end_sec: None,
            worker_id: None,
            cpu_us: 0,
            rows_in: 0,
            rows_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            latest_stats_sec: None,
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
    }

    /// Record a mid-execution scalar progress snapshot for an in-flight task.
    /// No-op if the task has already ended (snapshot is treated as stale) or
    /// if this snapshot is older than one already applied.
    ///
    /// Naming caveat: `cpu_us` here is **busy time** — cumulative wall-clock
    /// time spent inside `op.execute().await` across the task's local
    /// pipeline operators (the runtime stats layer stores it under
    /// `DURATION_KEY`). It is **not** elapsed wall-clock time since task
    /// submit (`end_sec - submit_sec`) that the frontend sometimes labels
    /// "duration". Two tasks with the same wall-clock duration can have very
    /// different busy times.
    pub fn update_task_stats(
        &mut self,
        entry: TaskStatsEntry,
        worker_id: Option<String>,
        timestamp_sec: f64,
    ) {
        let Some(task) = self.tasks.get_mut(&entry.task_id) else {
            return;
        };
        if !matches!(task.status, TaskStatus::Pending) {
            return;
        }
        if let Some(prev) = task.latest_stats_sec
            && prev > timestamp_sec
        {
            return;
        }
        // Apply the deltas to the parent group so totals track live tasks
        // rather than only updating when each task ends. `end_task` also
        // delta-updates so the running sum equals the per-task end value.
        let cpu_delta = entry.totals.cpu_us.saturating_sub(task.cpu_us);
        let rows_in_delta = entry.totals.rows_in.saturating_sub(task.rows_in);
        let rows_out_delta = entry.totals.rows_out.saturating_sub(task.rows_out);
        let bytes_in_delta = entry.totals.bytes_in.saturating_sub(task.bytes_in);
        let bytes_out_delta = entry.totals.bytes_out.saturating_sub(task.bytes_out);
        task.cpu_us = entry.totals.cpu_us;
        task.rows_in = entry.totals.rows_in;
        task.rows_out = entry.totals.rows_out;
        task.bytes_in = entry.totals.bytes_in;
        task.bytes_out = entry.totals.bytes_out;
        task.latest_stats_sec = Some(timestamp_sec);
        if task.worker_id.is_none() {
            task.worker_id = worker_id;
        }
        let key = Self::group_key_for_task(task);
        if let Some(&gi) = self.group_index.get(&key) {
            let g = &mut self.groups[gi];
            g.total_cpu_us += cpu_delta;
            g.total_rows_in += rows_in_delta;
            g.total_rows_out += rows_out_delta;
            g.total_bytes_in += bytes_in_delta;
            g.total_bytes_out += bytes_out_delta;
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
        let was_pending = was_submitted
            && matches!(
                self.tasks.get(&task_id).map(|t| &t.status),
                Some(TaskStatus::Pending)
            );
        // Mid-flight `update_task_stats` already increments group totals by
        // the per-tick delta, so end_task adds only the remaining delta
        // (final - last seen) to avoid double-counting.
        let (prev_cpu, prev_rows_in, prev_rows_out, prev_bytes_in, prev_bytes_out) = self
            .tasks
            .get(&task_id)
            .map(|t| (t.cpu_us, t.rows_in, t.rows_out, t.bytes_in, t.bytes_out))
            .unwrap_or((0, 0, 0, 0, 0));
        let cpu_us_delta = cpu_us.saturating_sub(prev_cpu);
        let rows_in_delta = rows_in.saturating_sub(prev_rows_in);
        let rows_out_delta = rows_out.saturating_sub(prev_rows_out);
        let bytes_in_delta = bytes_in.saturating_sub(prev_bytes_in);
        let bytes_out_delta = bytes_out.saturating_sub(prev_bytes_out);

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
        match &status {
            TaskStatus::Finished => group.finished_count += 1,
            TaskStatus::Failed { .. } => group.failed_count += 1,
            TaskStatus::Cancelled => group.cancelled_count += 1,
            TaskStatus::Pending => {}
        }

        group.total_cpu_us += cpu_us_delta;
        group.total_rows_in += rows_in_delta;
        group.total_rows_out += rows_out_delta;
        group.total_bytes_in += bytes_in_delta;
        group.total_bytes_out += bytes_out_delta;
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
            end_sec: None,
            worker_id: None,
            cpu_us: 0,
            rows_in: 0,
            rows_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            latest_stats_sec: None,
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
            TaskStatus::Pending => {}
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
        #[serde(skip_serializing)]
        results: Option<RecordBatch>,
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
        matches!(
            self,
            Self::Pending | Self::Optimizing { .. } | Self::Setup { .. } | Self::Executing { .. }
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

    fn finished() -> TaskStatus {
        TaskStatus::Finished
    }

    /// Two tasks with identical `node_ids` but different `name` strings should
    /// land in the same group. (Previously, group key included the name and
    /// these would split.)
    #[test]
    fn same_node_ids_different_names_collapse_to_one_group() {
        let mut store = TaskStore::default();
        store.submit_task(1, 7, vec![3, 5, 7], 0, Some("Limit(10)".to_string()), 0.0);
        store.submit_task(2, 7, vec![3, 5, 7], 0, Some("Limit(100)".to_string()), 0.0);

        assert_eq!(
            store.groups.len(),
            1,
            "expected one group, got {:?}",
            store.groups
        );
        assert_eq!(store.groups[0].task_count, 2);
        assert_eq!(store.groups[0].node_ids, vec![3, 5, 7]);
    }

    /// Two tasks with the same `name` but different `node_ids` should land in
    /// distinct groups (chain identity differs).
    #[test]
    fn different_node_ids_same_name_split_into_two_groups() {
        let mut store = TaskStore::default();
        store.submit_task(1, 5, vec![3, 5], 0, Some("Project".to_string()), 0.0);
        store.submit_task(2, 9, vec![7, 9], 0, Some("Project".to_string()), 0.0);

        assert_eq!(store.groups.len(), 2);
    }

    /// End-before-submit with empty node_ids should resolve to the same group
    /// as the eventual submit, so counts don't double up.
    #[test]
    fn end_before_submit_with_empty_node_ids_resolves_to_submit_group() {
        let mut store = TaskStore::default();

        // End arrives first, with empty node_ids — fallback creates a group
        // keyed by [last_node_id].
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
        store.submit_task(42, 7, vec![7], 0, Some("Filter".to_string()), 0.5);

        assert_eq!(store.groups.len(), 1);
        let g = &store.groups[0];
        // task_count should not double-count: end (no submit seen) credited 1,
        // submit found existing task and didn't increment further.
        assert_eq!(g.task_count, 1);
        assert_eq!(g.finished_count, 1);
    }
}
