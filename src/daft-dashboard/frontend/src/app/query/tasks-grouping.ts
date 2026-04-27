/**
 * Grouping helpers for the Flotilla Tasks sidebar.
 *
 * The server provides per-group aggregate summaries via `task_store.groups`
 * and a bounded set of retained individual tasks via `task_store.tasks`.
 * This module joins the two: each `TaskTypeRow` carries the server-computed
 * aggregates (always accurate, even when individual tasks have been evicted)
 * and attaches matching retained tasks for drill-down.
 */
import { OperatorInfo, OperatorStatus, TaskGroupSummary, TaskInfo, TaskStore } from "./types";

export type TaskRowStatus = OperatorStatus;

/**
 * Convert a TaskInfo status into the shared OperatorStatus vocabulary so the
 * sidebar can reuse the existing status chips/colors. Tasks are "Executing"
 * while they have a submit but no end time. Cancelled tasks are rendered as
 * Failed because the status chip vocabulary doesn't distinguish them.
 */
export function taskDisplayStatus(task: TaskInfo): TaskRowStatus {
  switch (task.status.status) {
    case "Pending":
      return task.end_sec != null ? "Finished" : "Executing";
    case "Finished":
      return "Finished";
    case "Failed":
      return "Failed";
    case "Cancelled":
      return "Failed";
  }
}

/** One row in the Tasks sidebar — a group of tasks that share a pipeline shape. */
export type TaskTypeRow = {
  /** Stable React key: `${origin_node_id}:${plan_fingerprint}`. */
  key: string;
  origin_node_id: number;
  origin_node_name: string;
  /** Distributed plan node names along the task's pipeline, leaf → head. */
  pipeline: string[];
  task_count: number;
  status_counts: Record<TaskRowStatus, number>;
  total_rows_in: number;
  total_rows_out: number;
  total_bytes_in: number;
  total_bytes_out: number;
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

/**
 * Derive a pipeline display from the group's name. The name is set by the
 * scheduler (e.g. "PhysicalScan->Limit") and is the grouping key.
 */
function pipelineForGroup(group: TaskGroupSummary): string[] {
  if (group.name.includes("->")) {
    return group.name.split("->");
  }
  return [group.name];
}

/** Maximum number of running tasks shown in the "top" section. */
export const TOP_K_RUNNING = 10;

/**
 * Extract active (running) tasks from the store, sorted by wall-clock duration
 * descending. TODO: sort by cpu_us instead once within-task metric updates land
 * (currently cpu_us is only populated on task end).
 */
export function getActiveTasks(taskStore: TaskStore | undefined): TaskInfo[] {
  if (!taskStore) return [];
  return Object.values(taskStore.tasks)
    .filter((t) => t.status.status === "Pending" && t.end_sec == null)
    .sort((a, b) => a.submit_sec - b.submit_sec); // oldest first = longest running
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

  // Index retained tasks by group key: (origin_node_id, name).
  const tasksByGroup = new Map<string, TaskInfo[]>();
  for (const t of Object.values(taskStore.tasks)) {
    const name = t.name ?? `Node ${t.origin_node_id}`;
    const key = `${t.origin_node_id}:${name}`;
    let arr = tasksByGroup.get(key);
    if (!arr) {
      arr = [];
      tasksByGroup.set(key, arr);
    }
    arr.push(t);
  }

  return taskStore.groups
    .map((g) => {
      const key = `${g.origin_node_id}:${g.name}`;
      const tasks = tasksByGroup.get(key) ?? [];
      const pipeline = pipelineForGroup(g);

      return {
        key,
        origin_node_id: g.origin_node_id,
        origin_node_name:
          operators[g.origin_node_id]?.node_info.name ??
          `Node ${g.origin_node_id}`,
        pipeline,
        task_count: g.task_count,
        status_counts: {
          Pending: 0,
          Executing: g.pending_count,
          Finished: g.finished_count,
          Failed: g.failed_count + g.cancelled_count,
        } as Record<TaskRowStatus, number>,
        total_rows_in: g.total_rows_in,
        total_rows_out: g.total_rows_out,
        total_bytes_in: g.total_bytes_in,
        total_bytes_out: g.total_bytes_out,
        total_cpu_sec: g.total_cpu_us / 1_000_000,
        first_start_sec: g.first_submit_sec,
        last_end_sec: g.pending_count > 0 ? null : (g.last_end_sec ?? null),
        tasks,
        retained_task_count: g.retained_task_count,
      };
    })
    .sort((a, b) => {
      if (a.origin_node_id !== b.origin_node_id)
        return b.origin_node_id - a.origin_node_id;
      return a.pipeline.join("->").localeCompare(b.pipeline.join("->"));
    });
}
