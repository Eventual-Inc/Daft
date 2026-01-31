import { AnimatedFish, Naruto } from "@/components/icons";
import { ExecutingState, OperatorStatus, Stat } from "./types";

const getStatusIcon = (status: OperatorStatus) => {
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

const getStatusText = (status: OperatorStatus) => {
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

const getStatusColor = (status: OperatorStatus) => {
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

// Get extra stats (all stats except the ones we're already displaying)
const formatStatValue = (stat: Stat) => {
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

export default function ProgressTable({
  exec_state,
}: {
  exec_state: ExecutingState;
}) {
  return (
    <div className="overflow-auto h-full">
      <div className="min-w-[770px]">
        {/* Table Headers */}
        <div className="bg-zinc-800 grid grid-cols-[50px_60px_100px_200px_120px_120px_1fr] gap-0 items-center min-h-[55px] border-b border-zinc-700">
          <div className="px-3 py-4 border-r border-zinc-700 h-full flex items-center"></div>
          <div className="px-3 py-4 text-sm font-bold text-white font-mono border-r border-zinc-700 h-full flex items-center justify-center">
            ID
          </div>
          <div className="px-3 py-4 text-right text-sm font-bold text-white font-mono border-r border-zinc-700 h-full flex items-center justify-end">
            Status
          </div>
          <div className="px-3 py-4 text-sm font-bold text-white font-mono border-r border-zinc-700 h-full flex items-center">
            Name
          </div>
          <div className="px-3 py-4 text-right text-sm font-bold text-white font-mono border-r border-zinc-700 h-full flex items-center justify-end">
            Rows In
          </div>
          <div className="px-3 py-4 text-right text-sm font-bold text-white font-mono border-r border-zinc-700 h-full flex items-center justify-end">
            Rows Out
          </div>
          <div className="px-3 py-4 text-sm font-bold text-white font-mono h-full flex items-center">
            Extra Stats
          </div>
        </div>

        {/* Operator Rows */}
        <div className="divide-y divide-zinc-700">
          {Object.entries(exec_state.exec_info.operators)
            .sort(([a], [b]) => parseInt(a) - parseInt(b))
            .map(([operatorId, operator]) => {
              const name = operator.node_info.name;
              // Extract important stats from operator.stats
              const rowsIn = operator.stats["rows in"]?.value || 0;
              const rowsOut = operator.stats["rows out"]?.value || 0;

              const extraStats = Object.entries(operator.stats)
                .filter(
                  ([key]) => !["rows in", "rows out", "cpu us"].includes(key)
                )
                .map(
                  ([key, stat]) =>
                    `${key.charAt(0).toUpperCase() + key.slice(1)}: ${formatStatValue(stat)}`
                )
                .join(", ");

              return (
                <div
                  key={operatorId}
                  className="grid grid-cols-[50px_60px_100px_200px_120px_120px_1fr] gap-0 items-center min-h-[55px] transition-colors hover:bg-zinc-800/50"
                >
                  <div className="px-3 py-4 flex items-center justify-end border-r border-zinc-700 h-full">
                    {getStatusIcon(operator.status)}
                  </div>
                  <div className="px-3 py-4 text-center text-sm text-zinc-400 font-mono border-r border-zinc-700 h-full flex items-center justify-center">
                    {operatorId}
                  </div>
                  <div className="pr-3 py-4 text-right text-sm border-r border-zinc-700 h-full flex items-center justify-end">
                    <span className={getStatusColor(operator.status)}>
                      {getStatusText(operator.status)}
                    </span>
                  </div>
                  <div className={`px-3 py-4 text-sm text-zinc-200 truncate border-r border-zinc-700 h-full flex items-center`}>
                    {name}
                  </div>
                  <div className="px-3 py-4 text-right text-sm text-zinc-300 font-mono border-r border-zinc-700 h-full flex items-center justify-end">
                    {name.includes("Scan") ? "-" : rowsIn.toLocaleString()}
                  </div>
                  <div className="px-3 py-4 text-right text-sm text-zinc-300 font-mono border-r border-zinc-700 h-full flex items-center justify-end">
                    {name.includes("Sink") ? "-" : rowsOut.toLocaleString()}
                  </div>
                  <div className="px-3 py-4 text-sm text-zinc-400 font-mono h-full flex items-center">
                    {extraStats || "-"}
                  </div>
                </div>
              );
            })}
        </div>
      </div>
    </div>
  );
}
