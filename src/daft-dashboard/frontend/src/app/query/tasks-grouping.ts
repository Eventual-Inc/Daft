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
  plan_fingerprint: number;
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
 * Derive a pipeline of human-readable node names from the group's node_ids.
 * Names come from the distributed plan (exec_info.operators). Falls back to
 * a retained task's name if available.
 */
function pipelineForGroup(
  group: TaskGroupSummary,
  retainedTasks: TaskInfo[],
  operators: Record<number, OperatorInfo>,
): string[] {
  // Check if a retained task has a backend-supplied name.
  const namedTask = retainedTasks.find((t) => t.name);
  if (namedTask?.name) {
    return namedTask.name.includes("->")
      ? namedTask.name.split("->")
      : [namedTask.name];
  }

  // Derive from the group's node_ids.
  if (group.node_ids.length > 0) {
    return group.node_ids.map(
      (id) => operators[id]?.node_info.name ?? `Node ${id}`,
    );
  }

  // Fall back to the origin node.
  return [
    operators[group.origin_node_id]?.node_info.name ??
      `Node ${group.origin_node_id}`,
  ];
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

  // Index retained tasks by group key.
  const tasksByGroup = new Map<string, TaskInfo[]>();
  for (const t of Object.values(taskStore.tasks)) {
    const key = `${t.origin_node_id}:${t.plan_fingerprint}`;
    let arr = tasksByGroup.get(key);
    if (!arr) {
      arr = [];
      tasksByGroup.set(key, arr);
    }
    arr.push(t);
  }

  return taskStore.groups
    .map((g) => {
      const key = `${g.origin_node_id}:${g.plan_fingerprint}`;
      const tasks = tasksByGroup.get(key) ?? [];
      const pipeline = pipelineForGroup(g, tasks, operators);

      return {
        key,
        origin_node_id: g.origin_node_id,
        origin_node_name:
          operators[g.origin_node_id]?.node_info.name ??
          `Node ${g.origin_node_id}`,
        pipeline,
        plan_fingerprint: g.plan_fingerprint,
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
        return a.origin_node_id - b.origin_node_id;
      return a.pipeline.join("->").localeCompare(b.pipeline.join("->"));
    });
}
