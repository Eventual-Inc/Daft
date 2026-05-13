import { AnimatedFish, Naruto } from "@/components/icons";
import { OperatorInfo, OperatorStatus, Stat, TaskStore } from "./types";
import { QueryStatusName } from "@/hooks/use-queries";

export const ROWS_IN_STAT_KEY = "rows.in";
export const ROWS_OUT_STAT_KEY = "rows.out";
export const BYTES_IN_STAT_KEY = "bytes.in";
export const BYTES_OUT_STAT_KEY = "bytes.out";
export const DURATION_US_STAT_KEY = "duration";

export const statNumericValue = (stat: Stat | undefined): number => {
  if (!stat) return 0;
  if (stat.type === "Duration") {
    return stat.value.secs + stat.value.nanos / 1e9;
  }
  return Number(stat.value) || 0;
};

/**
 * Compact human-readable row count: 1234 → "1.2K", 1234567 → "1.2M". Useful
 * for narrow table cells where the long-form `toLocaleString()` would wrap.
 */
export const formatCount = (count: number): string => {
  if (!isFinite(count) || count <= 0) return "0";
  if (count >= 1_000_000_000) {
    return `${(count / 1_000_000_000).toFixed(1)}B`;
  } else if (count >= 1_000_000) {
    return `${(count / 1_000_000).toFixed(1)}M`;
  } else if (count >= 1_000) {
    return `${(count / 1_000).toFixed(1)}K`;
  }
  return count.toString();
};

export const formatBytes = (bytes: number): string => {
  if (!isFinite(bytes) || bytes <= 0) return "0 B";
  if (bytes >= 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GiB`;
  } else if (bytes >= 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
  } else if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(1)} KiB`;
  } else {
    return `${Math.round(bytes)} B`;
  }
};

/**
 * Resolves the status to display for an operator, taking Flotilla task-level
 * failures into account. A "Finished" operator is never overridden — retried
 * tasks leave a non-zero failed_count even after the operator succeeds.
 */
export function getEffectiveStatus(
  operator: OperatorInfo | undefined,
  nodeId: number,
  taskStore: TaskStore | undefined,
  queryStatus: QueryStatusName = "Executing",
): OperatorStatus {
  if (!operator) return "Pending";

  if (taskStore) {
    const group = taskStore.groups.find((g) => g.node_ids.includes(nodeId));
    if (group) {
      // On a successful query, failed_count may be non-zero from Flotilla task
      // retries that ultimately succeeded — don't override the Finished status.
      if (group.failed_count > 0 && queryStatus !== "Finished") return "Failed";

      // When Daft terminates the query externally (Failed/Dead), tasks that
      // were still running never recorded an outcome — treat them as failed.
      if (group.running_count > 0 && (queryStatus === "Failed" || queryStatus === "Dead")) return "Failed";

      // Tasks cancelled as part of a user-initiated cancellation → gray.
      if (group.cancelled_count > 0 && queryStatus === "Canceled") return "Canceled";
    }
  }

  // Marked Finished by the backend but never actually started → never ran
  if (operator.status === "Finished" && !operator.start_sec) return "Pending";

  return operator.status;
}

export const getStatusIcon = (status: OperatorStatus, isTerminal = false) => {
  switch (status) {
    case "Finished":
      return <Naruto animated={!isTerminal} />;
    case "Executing":
      return <AnimatedFish animated={!isTerminal} />;
    case "Failed":
      return (
        <div className="w-5 h-5 flex items-center justify-center">
          <div className="w-4 h-4 bg-red-500 rounded-full flex items-center justify-center">
            <span className="text-white text-[10px] font-bold">!</span>
          </div>
        </div>
      );
    case "Canceled":
      return (
        <div className="w-5 h-5 flex items-center justify-center">
          <div className="w-4 h-4 bg-zinc-600 rounded-full flex items-center justify-center">
            <span className="text-zinc-300 text-[10px] font-bold">−</span>
          </div>
        </div>
      );
    case "Pending":
    default:
      return (
        <div className={`w-5 h-5 shrink-0 border-2 border-zinc-400 border-t-transparent rounded-full ${isTerminal ? "" : "animate-spin"}`}></div>
      );
  }
};

export const getStatusText = (status: OperatorStatus) => {
  switch (status) {
    case "Finished":  return "Finished";
    case "Executing": return "Running";
    case "Failed":    return "Failed";
    case "Canceled":  return "Canceled";
    default:          return "Pending";
  }
};

export const getStatusColor = (status: OperatorStatus) => {
  switch (status) {
    case "Finished":  return "text-green-500";
    case "Executing": return "text-(--daft-accent)";
    case "Failed":    return "text-red-500";
    case "Canceled":  return "text-zinc-400";
    default:          return "text-zinc-400";
  }
};

export const formatStatValue = (stat: Stat) => {
  switch (stat.type) {
    case "Count":
      return stat.value.toLocaleString();
    case "Bytes":
      return formatBytes(stat.value);
    case "Percent":
      return `${stat.value.toFixed(1)}%`;
    case "Duration":
      const totalSec = stat.value.secs + stat.value.nanos / 1e9;
      return formatDuration(totalSec);
    case "Float":
      return stat.value.toFixed(2);
    default:
      return String((stat as any).value);
  }
};

export const formatDuration = (seconds: number): string => {
  if (seconds < 0.001) {
    return `${(seconds * 1_000_000).toFixed(0)}µs`;
  } else if (seconds < 1) {
    return `${(seconds * 1000).toFixed(0)}ms`;
  } else if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  } else if (seconds < 3600) {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}m ${secs.toFixed(0)}s`;
  } else {
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    return `${hrs}h ${mins}m`;
  }
};

export const getStatusBorderColor = (status: OperatorStatus) => {
  switch (status) {
    case "Finished":  return "border-green-600";
    case "Executing": return "border-orange-500";
    case "Failed":    return "border-red-600";
    case "Canceled":  return "border-zinc-600";
    default:          return "border-zinc-600";
  }
};
