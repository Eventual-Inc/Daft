import { AnimatedFish, Naruto } from "@/components/icons";
import { OperatorStatus, Stat } from "./types";

export const ROWS_IN_STAT_KEY = "rows.in";
export const ROWS_OUT_STAT_KEY = "rows.out";
export const DURATION_US_STAT_KEY = "duration";

export const getStatusIcon = (status: OperatorStatus) => {
  switch (status) {
    case "Finished":
      return <Naruto />;
    case "Executing":
      return <AnimatedFish />;
    case "Failed":
      return (
        <div className="w-5 h-5 flex items-center justify-center">
          <div className="w-4 h-4 bg-red-500 rounded-full flex items-center justify-center">
            <span className="text-white text-[10px] font-bold">!</span>
          </div>
        </div>
      );
    case "Pending":
    default:
      return (
        <div className="w-5 h-5 border-2 border-zinc-400 border-t-transparent rounded-full animate-spin"></div>
      );
  }
};

export const getStatusText = (status: OperatorStatus) => {
  if (status === "Finished") {
    return "Finished";
  } else if (status === "Executing") {
    return "Running";
  } else if (status === "Failed") {
    return "Failed";
  } else {
    return "Pending";
  }
};

export const getStatusColor = (status: OperatorStatus) => {
  switch (status) {
    case "Finished":
      return "text-green-500";
    case "Executing":
      return "text-(--daft-accent)";
    case "Failed":
      return "text-red-500";
    case "Pending":
    default:
      return "text-zinc-400";
  }
};

export const formatStatValue = (stat: Stat) => {
  switch (stat.type) {
    case "Count":
      return stat.value.toLocaleString();
    case "Bytes":
      const bytes = stat.value;
      if (bytes >= 1024 * 1024 * 1024) {
        return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GiB`;
      } else if (bytes >= 1024 * 1024) {
        return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
      } else if (bytes >= 1024) {
        return `${(bytes / 1024).toFixed(1)} KiB`;
      } else {
        return `${bytes} B`;
      }
    case "Percent":
      return `${stat.value.toFixed(1)}%`;
    case "Duration":
      return `${stat.value.toFixed(1)}s`;
    case "Float":
      return stat.value.toFixed(2);
    default:
      return String((stat as any).value);
  }
};

export const getStatusBorderColor = (status: OperatorStatus) => {
  switch (status) {
    case "Finished":
      return "border-green-600";
    case "Executing":
      return "border-orange-500";
    case "Failed":
      return "border-red-600";
    case "Pending":
    default:
      return "border-zinc-600";
  }
};
