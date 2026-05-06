/**
 * Grouping helpers for the Flotilla Tasks sidebar.
 *
 * The server provides per-group aggregate summaries via `task_store.groups`
 * and a bounded set of retained individual tasks via `task_store.tasks`.
 * This module joins the two: each `TaskTypeRow` carries the server-computed
 * aggregates (always accurate, even when individual tasks have been evicted)
 * and attaches matching retained tasks for drill-down.
 *
 * Groups are keyed by the full `node_ids` chain (the distributed pipeline
 * nodes that contributed to the fused task) — two tasks with the same chain
 * belong to the same group regardless of how their `name` strings differ
 * (e.g. parameter-driven variations like different limit values).
 */
import { OperatorInfo, OperatorStatus, TaskInfo, TaskStore } from "./types";

export type TaskRowStatus = OperatorStatus;

/**
 * Convert a TaskInfo status into the shared OperatorStatus vocabulary so the
 * sidebar can reuse the existing status chips/colors. Cancelled tasks are
 * rendered as Failed because the status chip vocabulary doesn't distinguish
 * them. Running tasks map to Executing; Pending stays Pending so the UI can
 * distinguish "submitted but not yet executing" from "actually executing".
 */
export function taskDisplayStatus(task: TaskInfo): TaskRowStatus {
  switch (task.status.status) {
    case "Pending":
      return task.end_sec != null ? "Finished" : "Pending";
    case "Running":
      return task.end_sec != null ? "Finished" : "Executing";
    case "Finished":
      return "Finished";
    case "Failed":
      return "Failed";
    case "Cancelled":
      return "Failed";
  }
}

/**
 * Wall-clock duration of an in-flight task in seconds. Pending tasks (those
 * that have been submitted but haven't started executing yet) have no
 * meaningful duration; we return 0 for sort stability but callers should
 * suppress the duration display via {@link taskHasStarted}.
 */
export function taskDurationSec(task: TaskInfo, nowSec: number): number {
  if (task.start_sec == null) return 0;
  const end = task.end_sec ?? nowSec;
  return Math.max(0, end - task.start_sec);
}

export function taskHasStarted(task: TaskInfo): boolean {
  return task.start_sec != null;
}

/** A node in a distributed-plan chain, displayable + clickable. */
export type PlanChainNode = { id: number; name: string };

/** One row in the Tasks sidebar — a group of tasks that share a node_ids chain. */
export type TaskTypeRow = {
  /** Stable React key: stringified node_ids. */
  key: string;
  /** The dispatching distributed plan node (= node_ids.last()). */
  last_node_id: number;
  /** Display name for the dispatcher (used in headers/sort). */
  last_node_name: string;
  /** Local-plan chain (`task.name` split on "->"), used by the "Local Plan" column. */
  pipeline: string[];
  /** Distributed-plan chain derived from node_ids, used by the "Distributed Plan" column. */
  distributed_plan: PlanChainNode[];
  /** The full chain — directly used by the filter predicate and hover handler. */
  node_ids: number[];
  task_count: number;
  status_counts: Record<TaskRowStatus, number>;
  total_cpu_sec: number;
  /** Earliest task submit. */
  first_start_sec: number;
  /** Latest task end, or null if any task is still running. */
  last_end_sec: number | null;
  /** Retained individual tasks for this group (may be fewer than task_count). */
  tasks: TaskInfo[];
  /** How many individual tasks for this group are retained (active + failed + top-K). */
  retained_task_count: number;
};

/** Stable string key for indexing by node_ids chain. */
function nodeIdsKey(nodeIds: number[]): string {
  return nodeIds.join(",");
}

/**
 * Derive the distributed-plan chain (id + display name) for a node_ids list,
 * looking each id up in the operators map. Falls back to "Node {id}" when the
 * operator isn't yet in the map (e.g. SSE delivery race).
 */
function distributedPlanChain(
  nodeIds: number[],
  operators: Record<number, OperatorInfo>,
): PlanChainNode[] {
  return nodeIds.map((id) => ({
    id,
    name: operators[id]?.node_info.name ?? `Node ${id}`,
  }));
}

/** Local-plan chain from a task name like `"ScanTaskSource->Project"`. */
function localPlanChain(name: string | null | undefined): string[] {
  if (!name) return [];
  return name.includes("->") ? name.split("->") : [name];
}

/** Maximum number of in-flight (running + pending) tasks in the "top" section. */
export const TOP_K_RUNNING = 10;

/**
 * Extract in-flight (running + pending) tasks from the store. Running tasks
 * sort first, ordered by wall-clock duration descending; pending tasks sort
 * after, ordered by submit time ascending. With this ordering, pending tasks
 * only appear in the top-K once running tasks no longer fill it.
 *
 * TODO: sort running by cpu_us instead once within-task metric updates land
 * (currently cpu_us is only populated on task end).
 */
export function getActiveTasks(taskStore: TaskStore | undefined): TaskInfo[] {
  if (!taskStore) return [];
  return Object.values(taskStore.tasks)
    .filter(
      (t) =>
        (t.status.status === "Pending" || t.status.status === "Running") &&
        t.end_sec == null,
    )
    .sort((a, b) => {
      const aRunning = a.status.status === "Running" ? 0 : 1;
      const bRunning = b.status.status === "Running" ? 0 : 1;
      if (aRunning !== bRunning) return aRunning - bRunning;
      if (aRunning === 0) {
        // Both running: oldest start first (= longest running first).
        return (a.start_sec ?? 0) - (b.start_sec ?? 0);
      }
      // Both pending: oldest submit first.
      return a.submit_sec - b.submit_sec;
    });
}

/**
 * Build task-type rows from the server-provided TaskStore.
 *
 * Group summaries provide accurate aggregate stats (counts, totals) even when
 * individual tasks have been evicted from the retained set. Retained tasks are
 * attached for drill-down when the user expands a row.
 */
export function buildTaskRows(
  taskStore: TaskStore | undefined,
  operators: Record<number, OperatorInfo>,
): TaskTypeRow[] {
  if (!taskStore) return [];

  // Index retained tasks by node_ids chain (matching the server's group key).
  const tasksByGroup = new Map<string, TaskInfo[]>();
  for (const t of Object.values(taskStore.tasks)) {
    const ids = t.node_ids.length > 0 ? t.node_ids : [t.last_node_id];
    const key = nodeIdsKey(ids);
    let arr = tasksByGroup.get(key);
    if (!arr) {
      arr = [];
      tasksByGroup.set(key, arr);
    }
    arr.push(t);
  }

  return taskStore.groups
    .map((g) => {
      const ids = g.node_ids.length > 0 ? g.node_ids : [g.last_node_id];
      const key = nodeIdsKey(ids);
      const tasks = tasksByGroup.get(key) ?? [];

      // Local plan: derived from any retained task's `name`, falling back to
      // the server-supplied display label (`g.name`). Empty if nothing usable.
      const sampleName = tasks[0]?.name ?? g.name;
      const pipeline = localPlanChain(sampleName);

      const inflight = g.pending_count + g.running_count;
      return {
        key,
        last_node_id: g.last_node_id,
        last_node_name:
          operators[g.last_node_id]?.node_info.name ??
          `Node ${g.last_node_id}`,
        pipeline,
        distributed_plan: distributedPlanChain(ids, operators),
        node_ids: ids,
        task_count: g.task_count,
        status_counts: {
          Pending: g.pending_count,
          Executing: g.running_count,
          Finished: g.finished_count,
          Failed: g.failed_count + g.cancelled_count,
        } as Record<TaskRowStatus, number>,
        total_cpu_sec: g.total_cpu_us / 1_000_000,
        first_start_sec: g.first_submit_sec,
        last_end_sec: inflight > 0 ? null : (g.last_end_sec ?? null),
        tasks,
        retained_task_count: g.retained_task_count,
      };
    })
    .sort((a, b) => {
      if (a.last_node_id !== b.last_node_id)
        return b.last_node_id - a.last_node_id;
      return a.key.localeCompare(b.key);
    });
}
