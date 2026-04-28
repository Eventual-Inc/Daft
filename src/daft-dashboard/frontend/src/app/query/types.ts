export type PlanInfo = {
  plan_start_sec: number;
  plan_end_sec: number;
  optimized_plan: string;
};

export type OperatorStatus = "Pending" | "Executing" | "Finished" | "Failed";

export type NodeInfo = {
  name: string;
  id: number;
  node_category: "Intermediate" | "Source" | "StreamingSink" | "BlockingSink";
  context: Record<string, string>;
};

export type DurationValue = {
  secs: number;
  nanos: number;
};

export type Stat =
  | {
      type: "Count";
      value: number;
    }
  | {
      type: "Bytes";
      value: number;
    }
  | {
      type: "Percent";
      value: number;
    }
  | {
      type: "Float";
      value: number;
    }
  | {
      type: "Duration";
      value: DurationValue;
    };

export type OperatorInfo = {
  status: OperatorStatus;
  node_info: NodeInfo;
  stats: Record<string, Stat>;
  start_sec?: number;
  end_sec?: number;
};

export type PhysicalPlanNode = {
  id: number;
  name: string;
  type: string;
  category: string;
  children?: PhysicalPlanNode[];
};

/**
 * Task lifecycle info pushed by the Flotilla scheduler.
 *
 * A Flotilla task is a fused chain of local plan nodes dispatched to a
 * Swordfish worker. Tasks originate at a specific distributed plan node
 * (`origin_node_id`) and tasks with the same fused pipeline share a
 * `plan_fingerprint`, so the UI groups by (origin_node_id, plan_fingerprint).
 */
export type TaskStatus =
  | { status: "Pending" }
  | { status: "Finished" }
  | { status: "Failed"; message: string | null }
  | { status: "Cancelled" };

export type TaskInfo = {
  task_id: number;
  origin_node_id: number;
  node_ids: number[];
  plan_fingerprint: number;
  name?: string;
  status: TaskStatus;
  submit_sec: number;
  end_sec?: number;
  worker_id?: string;
  cpu_us: number;
};

/** Server-side aggregate summary for a group of tasks sharing an (origin_node_id, pipeline_name). */
export type TaskGroupSummary = {
  origin_node_id: number;
  node_ids: number[];
  name: string;
  task_count: number;
  pending_count: number;
  finished_count: number;
  failed_count: number;
  cancelled_count: number;
  total_cpu_us: number;
  first_submit_sec: number;
  last_end_sec?: number;
  /** How many individual tasks for this group are in the retained `tasks` map. */
  retained_task_count: number;
};

/**
 * Bounded task store. Contains per-group aggregate summaries (always accurate)
 * and a bounded set of retained individual tasks (active, failed, and top-K
 * longest-duration completed tasks per group).
 */
export type TaskStore = {
  groups: TaskGroupSummary[];
  tasks: Record<number, TaskInfo>;
};

export type ExecInfo = {
  exec_start_sec: number;
  operators: Record<number, OperatorInfo>;
  physical_plan: string;
  /** Bounded task store. Empty for Swordfish (local) queries. */
  task_store?: TaskStore;
  // TODO: Logs
};

export type ExecutingState = {
  status: "Executing";
  plan_info: PlanInfo;
  exec_info: ExecInfo;
};

export type QueryState =
  | {
      status: "Pending";
    }
  | {
      status: "Optimizing";
      plan_start_sec: number;
    }
  | {
      status: "Setup";
      plan_info: PlanInfo;
    }
  | ExecutingState
  | {
      status: "Finalizing";
      plan_info: PlanInfo;
      exec_info: ExecInfo;
      exec_end_sec: number;
    }
  | {
      status: "Finished";
      plan_info: PlanInfo;
      exec_info: ExecInfo;
      exec_end_sec: number;
      end_sec: number;
    }
  | {
      status: "Failed";
      end_sec: number;
      message: string;
    }
  | {
      status: "Canceled";
      end_sec: number;
      message: string;
    }
  | {
      status: "Dead";
      plan_info: PlanInfo | null;
      exec_info: ExecInfo | null;
      marked_dead_sec: number;
    };

export type QueryInfo = {
  id: string;
  start_sec: number;
  last_heartbeat_sec: number;
  unoptimized_plan: string;
  runner: string;
  ray_dashboard_url?: string;
  entrypoint?: string;
  python_version?: string;
  daft_version?: string;
  ray_version?: string;
  state: QueryState;
};

export type ResultPreview = {
  html: string | null;
  num_rows: number;
  total_rows: number | null;
};
