export type PlanInfo = {
  plan_start_sec: number;
  plan_end_sec: number;
  optimized_plan: string;
};

export type OperatorStatus = "Pending" | "Executing" | "Finished";

export type NodeInfo = {
  name: string;
  id: number;
  node_category: "Intermediate" | "Source" | "StreamingSink" | "BlockingSink";
  context: Record<string, string>;
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
      value: number;
    };

export type OperatorInfo = {
  status: OperatorStatus;
  node_info: NodeInfo;
  stats: Record<string, Stat>;
};

export type ExecInfo = {
  exec_start_sec: number;
  operators: Record<number, OperatorInfo>;
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
    };

export type QueryInfo = {
  id: string;
  start_sec: number;
  unoptimized_plan: string;
  state: QueryState;
};
