/**
 * Mock data for the Flotilla Tasks tab.
 *
 * A Flotilla query is a distributed plan whose nodes each dispatch one or more
 * "swordfish tasks" — a fused chain of local plan nodes (e.g. Read → Project)
 * shipped to a Swordfish worker. Tasks with the same local pipeline shape
 * (same chain of local node types) share a `plan_fingerprint` and reuse the
 * same compiled pipeline at the worker.
 *
 * For the UI we group tasks by (origin_node_id, plan_fingerprint):
 *   - same origin + same fingerprint       -> one row
 *   - different origin, same fingerprint   -> different rows
 *     (e.g. two ScanSource nodes both doing Read→Project)
 *
 * TODO(dashboard): when the real data pipeline lands, replace
 * `generateMockTasks` with an event stream pushed from the Flotilla scheduler.
 * The data shape below is what the real backend will need to supply.
 */
import { OperatorInfo, OperatorStatus } from "./types";

/**
 * One swordfish task: a single execution of a fused local pipeline on a
 * Swordfish worker. The shape intentionally mirrors what the distributed
 * scheduler tracks today (see `TaskContext` in daft-distributed).
 */
export type SwordfishTask = {
  task_id: number;
  /** Distributed plan node that dispatched this task. */
  origin_node_id: number;
  /** Local plan node names in pipeline order (first is leaf, last is head). */
  pipeline: string[];
  /** Hash of the fused pipeline shape — identical for tasks that share a pipeline. */
  plan_fingerprint: number;
  /** Worker on which the task ran. */
  worker_id: string;
  /** Worker IP — handy for per-worker skew analysis. */
  ip_address: string;
  status: OperatorStatus;
  start_sec: number;
  end_sec: number | null;
  rows_in: number;
  rows_out: number;
  bytes_in: number;
  bytes_out: number;
  cpu_sec: number;
  /** Per-task inputs: filename for reads, partition id, shuffle key range, etc. */
  context: Record<string, string>;
};

/** One row in the Tasks tab table — a group of tasks with the same pipeline shape. */
export type TaskTypeRow = {
  /** Stable key for React: `${origin_node_id}:${plan_fingerprint}`. */
  key: string;
  origin_node_id: number;
  origin_node_name: string;
  pipeline: string[];
  plan_fingerprint: number;
  task_count: number;
  status_counts: Record<OperatorStatus, number>;
  total_rows_in: number;
  total_rows_out: number;
  total_bytes_in: number;
  total_bytes_out: number;
  total_cpu_sec: number;
  /** Earliest task start. */
  first_start_sec: number;
  /** Latest task end, or null if any task is still running. */
  last_end_sec: number | null;
  tasks: SwordfishTask[];
};

// ---------------------------------------------------------------------------
// Deterministic PRNG so the mock is stable across renders for a given query.
// ---------------------------------------------------------------------------
function hashStr(s: string): number {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h >>> 0;
}

function mulberry32(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const randInt = (rng: () => number, lo: number, hi: number) =>
  Math.floor(rng() * (hi - lo + 1)) + lo;

// ---------------------------------------------------------------------------
// Pipeline shapes per distributed node type.
// Each physical plan node type maps to one or more plausible fused pipelines.
// Returning multiple pipelines lets a single origin produce multiple rows in
// the task-types table (e.g. a ScanSource that emits both a cold-read pipeline
// and a re-used pipeline from the metadata cache).
// ---------------------------------------------------------------------------
function pipelinesForNode(name: string, category: string): string[][] {
  const n = name || "";
  if (category === "Source" || /Scan|Read|InMemory|Glob/i.test(n)) {
    return [
      ["ScanTaskSource", "Project"],
      ["ScanTaskSource"],
    ];
  }
  if (/Shuffle|Exchange|Repartition/i.test(n)) {
    return [["FlightShuffleRead", "Project"]];
  }
  if (/Aggregate|GroupBy/i.test(n)) {
    return [["HashAggregate"]];
  }
  if (/Sort|TopN/i.test(n)) {
    return [["Sort"]];
  }
  if (/Join/i.test(n)) {
    return [["HashJoinProbe", "Project"]];
  }
  if (/UDF|VLLM|ActorPool/i.test(n)) {
    return [["Project", n]];
  }
  if (/Write|Sink/i.test(n)) {
    return [["Project", "Write"]];
  }
  if (/Filter/i.test(n)) {
    return [["Project", "Filter"]];
  }
  if (/Limit|Sample/i.test(n)) {
    return [["Limit"]];
  }
  // Generic fallthrough — single-node pipeline using the node's own name.
  return [[n || "Project"]];
}

/** Synthesize a plausible per-task context (filename, partition id, etc.). */
function contextForTask(
  pipeline: string[],
  taskIdx: number,
  rng: () => number,
): Record<string, string> {
  const leaf = pipeline[0];
  if (leaf === "ScanTaskSource") {
    const files = [
      "s3://my-bucket/events/2024-01-01/part-00000.parquet",
      "s3://my-bucket/events/2024-01-01/part-00001.parquet",
      "s3://my-bucket/events/2024-01-02/part-00000.parquet",
      "s3://my-bucket/events/2024-01-02/part-00001.parquet",
      "s3://my-bucket/events/2024-01-03/part-00000.parquet",
      "s3://my-bucket/events/2024-01-04/part-00000.parquet",
    ];
    return {
      file: files[taskIdx % files.length],
      row_groups: `${randInt(rng, 1, 4)}`,
    };
  }
  if (leaf === "FlightShuffleRead") {
    return {
      shuffle_partition: `${taskIdx}`,
      upstream_tasks: `${randInt(rng, 4, 16)}`,
    };
  }
  if (leaf === "HashAggregate") {
    return { partition: `${taskIdx}`, grouping_keys: "user_id, event_day" };
  }
  if (leaf === "HashJoinProbe") {
    return { partition: `${taskIdx}`, build_side_rows: `${randInt(rng, 10_000, 1_000_000)}` };
  }
  return { partition: `${taskIdx}` };
}

// ---------------------------------------------------------------------------
// Mock generation.
// ---------------------------------------------------------------------------
/**
 * Generate a deterministic set of mock tasks for the given operators.
 * `seedKey` (typically the query id) keeps the mock stable across re-renders.
 */
export function generateMockTasks(
  operators: Record<number, OperatorInfo>,
  execStartSec: number,
  seedKey: string,
): SwordfishTask[] {
  const tasks: SwordfishTask[] = [];
  let nextTaskId = 1000;
  const workers = [
    { id: "worker-a4f2", ip: "10.0.1.12" },
    { id: "worker-b31e", ip: "10.0.1.13" },
    { id: "worker-c0de", ip: "10.0.1.14" },
    { id: "worker-d97a", ip: "10.0.1.15" },
  ];

  const rng = mulberry32(hashStr(seedKey));
  const now = Date.now() / 1000;

  const entries = Object.entries(operators)
    .map(([id, op]) => ({ id: Number(id), op }))
    .sort((a, b) => a.id - b.id);

  for (const { id, op } of entries) {
    const pipelines = pipelinesForNode(op.node_info.name, op.node_info.node_category);
    // Most nodes only use the first pipeline. Give sources a 50% chance of
    // producing two so we can exercise the multi-row-per-origin case.
    const usedPipelines =
      op.node_info.node_category === "Source" && rng() > 0.5
        ? pipelines
        : [pipelines[0]];

    for (const pipeline of usedPipelines) {
      const fingerprint = hashStr(pipeline.join("->"));
      const nTasks = randInt(rng, 3, 12);
      // Base stats scale with the operator's own reported volume, if any.
      const opRowsOut = (op.stats["rows.out"]?.value as number | undefined) ?? 0;
      const opBytesOut =
        (op.stats["bytes.out"]?.value as number | undefined) ?? 0;
      const perTaskRows = opRowsOut > 0 ? Math.max(1, Math.floor(opRowsOut / nTasks)) : randInt(rng, 50_000, 500_000);
      const perTaskBytes = opBytesOut > 0 ? Math.max(1, Math.floor(opBytesOut / nTasks)) : randInt(rng, 5_000_000, 80_000_000);

      for (let i = 0; i < nTasks; i++) {
        // Decide status from the parent operator's status so the tab feels
        // consistent with the Execution tab.
        let status: OperatorStatus;
        if (op.status === "Finished") {
          status = "Finished";
        } else if (op.status === "Failed") {
          status = i === 0 ? "Failed" : "Finished";
        } else if (op.status === "Executing") {
          const r = rng();
          status = r < 0.5 ? "Finished" : r < 0.85 ? "Executing" : "Pending";
        } else {
          status = "Pending";
        }

        const worker = workers[randInt(rng, 0, workers.length - 1)];
        const start = (op.start_sec ?? execStartSec) + rng() * 2;
        const duration = 0.2 + rng() * 4.5;
        const end =
          status === "Pending"
            ? null
            : status === "Executing"
              ? null
              : start + duration;
        const rowsOut = Math.max(1, Math.floor(perTaskRows * (0.7 + rng() * 0.6)));
        const rowsIn = pipeline[0] === "ScanTaskSource" ? 0 : Math.floor(rowsOut * (0.9 + rng() * 0.3));
        const bytesOut = Math.max(1, Math.floor(perTaskBytes * (0.7 + rng() * 0.6)));
        const bytesIn = pipeline[0] === "ScanTaskSource" ? 0 : Math.floor(bytesOut * (0.9 + rng() * 0.3));
        const cpu =
          status === "Pending"
            ? 0
            : status === "Executing"
              ? (now - start) * (0.6 + rng() * 0.3)
              : duration * (0.6 + rng() * 0.3);

        tasks.push({
          task_id: nextTaskId++,
          origin_node_id: id,
          pipeline,
          plan_fingerprint: fingerprint,
          worker_id: worker.id,
          ip_address: worker.ip,
          status,
          start_sec: start,
          end_sec: end,
          rows_in: rowsIn,
          rows_out: rowsOut,
          bytes_in: bytesIn,
          bytes_out: bytesOut,
          cpu_sec: cpu,
          context: contextForTask(pipeline, i, rng),
        });
      }
    }
  }

  return tasks;
}

/** Group tasks into task-type rows by (origin_node_id, plan_fingerprint). */
export function groupTasks(
  tasks: SwordfishTask[],
  operators: Record<number, OperatorInfo>,
): TaskTypeRow[] {
  const byKey = new Map<string, TaskTypeRow>();

  for (const t of tasks) {
    const key = `${t.origin_node_id}:${t.plan_fingerprint}`;
    let row = byKey.get(key);
    if (!row) {
      row = {
        key,
        origin_node_id: t.origin_node_id,
        origin_node_name:
          operators[t.origin_node_id]?.node_info.name ?? `Node ${t.origin_node_id}`,
        pipeline: t.pipeline,
        plan_fingerprint: t.plan_fingerprint,
        task_count: 0,
        status_counts: { Pending: 0, Executing: 0, Finished: 0, Failed: 0 },
        total_rows_in: 0,
        total_rows_out: 0,
        total_bytes_in: 0,
        total_bytes_out: 0,
        total_cpu_sec: 0,
        first_start_sec: t.start_sec,
        last_end_sec: t.end_sec,
        tasks: [],
      };
      byKey.set(key, row);
    }

    row.task_count += 1;
    row.status_counts[t.status] += 1;
    row.total_rows_in += t.rows_in;
    row.total_rows_out += t.rows_out;
    row.total_bytes_in += t.bytes_in;
    row.total_bytes_out += t.bytes_out;
    row.total_cpu_sec += t.cpu_sec;
    row.first_start_sec = Math.min(row.first_start_sec, t.start_sec);
    // If any task is still in flight, the group's end is null.
    if (t.end_sec == null) {
      row.last_end_sec = null;
    } else if (row.last_end_sec != null) {
      row.last_end_sec = Math.max(row.last_end_sec, t.end_sec);
    }
    row.tasks.push(t);
  }

  // Stable ordering: by origin node id, then by pipeline string.
  return Array.from(byKey.values()).sort((a, b) => {
    if (a.origin_node_id !== b.origin_node_id)
      return a.origin_node_id - b.origin_node_id;
    return a.pipeline.join("->").localeCompare(b.pipeline.join("->"));
  });
}
