/**
 * Grouping helpers for the Flotilla Tasks sidebar.
 *
 * Tasks arrive from the backend via `exec_info.tasks` (keyed by task_id).
 * The sidebar groups them by (origin_node_id, plan_fingerprint):
 *   - same origin + same fingerprint       -> one row
 *   - different origin, same fingerprint   -> different rows
 *     (e.g. two ScanSource nodes both doing Read→Project)
 *
 * These helpers shape the raw task stream into the grouped rows the table
 * renders. The task-level info lives in `TaskInfo` (see types.ts).
 */
import { OperatorInfo, OperatorStatus, TaskInfo } from "./types";

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
  tasks: TaskInfo[];
};

/**
 * Derive a pipeline of human-readable node names from the task's node_ids.
 * Names come from the distributed plan (exec_info.operators). If a name is
 * unknown the id is shown as a fallback so rows still group deterministically.
 */
function pipelineForTask(
  task: TaskInfo,
  operators: Record<number, OperatorInfo>,
): string[] {
  if (task.name) {
    // Backend-supplied name overrides the derived pipeline. Treat a dash-
    // delimited name (`Read->Project`) as a pipeline; otherwise use as-is.
    return task.name.includes("->") ? task.name.split("->") : [task.name];
  }
  if (task.node_ids.length === 0) {
    return [operators[task.origin_node_id]?.node_info.name ?? `Node ${task.origin_node_id}`];
  }
  return task.node_ids.map(
    (id) => operators[id]?.node_info.name ?? `Node ${id}`,
  );
}

export function groupTasks(
  tasks: TaskInfo[],
  operators: Record<number, OperatorInfo>,
): TaskTypeRow[] {
  const byKey = new Map<string, TaskTypeRow>();

  for (const t of tasks) {
    const key = `${t.origin_node_id}:${t.plan_fingerprint}`;
    const pipeline = pipelineForTask(t, operators);
    const status = taskDisplayStatus(t);
    const cpuSec = t.cpu_us / 1_000_000;

    let row = byKey.get(key);
    if (!row) {
      row = {
        key,
        origin_node_id: t.origin_node_id,
        origin_node_name:
          operators[t.origin_node_id]?.node_info.name ?? `Node ${t.origin_node_id}`,
        pipeline,
        plan_fingerprint: t.plan_fingerprint,
        task_count: 0,
        status_counts: { Pending: 0, Executing: 0, Finished: 0, Failed: 0 },
        total_rows_in: 0,
        total_rows_out: 0,
        total_bytes_in: 0,
        total_bytes_out: 0,
        total_cpu_sec: 0,
        first_start_sec: t.submit_sec,
        last_end_sec: t.end_sec ?? null,
        tasks: [],
      };
      byKey.set(key, row);
    }

    row.task_count += 1;
    row.status_counts[status] += 1;
    row.total_rows_in += t.rows_in;
    row.total_rows_out += t.rows_out;
    row.total_bytes_in += t.bytes_in;
    row.total_bytes_out += t.bytes_out;
    row.total_cpu_sec += cpuSec;
    row.first_start_sec = Math.min(row.first_start_sec, t.submit_sec);
    if (t.end_sec == null) {
      row.last_end_sec = null;
    } else if (row.last_end_sec != null) {
      row.last_end_sec = Math.max(row.last_end_sec, t.end_sec);
    }
    row.tasks.push(t);
  }

  return Array.from(byKey.values()).sort((a, b) => {
    if (a.origin_node_id !== b.origin_node_id)
      return a.origin_node_id - b.origin_node_id;
    return a.pipeline.join("->").localeCompare(b.pipeline.join("->"));
  });
}
