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

export type ShuffleInfo = {
  strategy: string;
  num_partitions: number;
  backend: string;
};

export type PhysicalPlanNode = {
  id: number;
  name: string;
  type: string;
  category: string;
  children?: PhysicalPlanNode[];
  is_shuffle_boundary?: boolean;
  shuffle_info?: ShuffleInfo;
};

export type ExecInfo = {
  exec_start_sec: number;
  operators: Record<number, OperatorInfo>;
  physical_plan: string;
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
