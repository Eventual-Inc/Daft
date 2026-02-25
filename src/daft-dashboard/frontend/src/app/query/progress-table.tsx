import { useState, useEffect } from "react";
import { ExecutingState, OperatorInfo } from "./types";
import {
  getStatusIcon,
  getStatusText,
  getStatusColor,
  formatStatValue,
  formatDuration,
  ROWS_IN_STAT_KEY,
  ROWS_OUT_STAT_KEY,
  DURATION_US_STAT_KEY,
} from "./stats-utils";

function OperatorDuration({ operator }: { operator: OperatorInfo }) {
  const [now, setNow] = useState(() => Date.now() / 1000);
  const isExecuting = operator.status === "Executing";

  useEffect(() => {
    if (!isExecuting) return;
    const id = setInterval(() => setNow(Date.now() / 1000), 200);
    return () => clearInterval(id);
  }, [isExecuting]);

  if (!operator.start_sec) return <>-</>;
  const end = operator.end_sec ?? (isExecuting ? now : null);
  if (end == null) return <>-</>;
  return <>{formatDuration(Math.max(0, end - operator.start_sec))}</>;
}

export default function ProgressTable({
  exec_state,
}: {
  exec_state: ExecutingState;
}) {
  return (
    <div className="overflow-auto h-full">
      <div className="min-w-[870px]">
        {/* Table Headers */}
        <div className="bg-zinc-800 grid grid-cols-[50px_60px_100px_200px_120px_120px_100px_1fr] gap-0 items-center min-h-[55px] border-b border-zinc-700">
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
          <div className="px-3 py-4 text-right text-sm font-bold text-white font-mono border-r border-zinc-700 h-full flex items-center justify-end">
            Duration
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
              const rowsIn = operator.stats[ROWS_IN_STAT_KEY]?.value || 0;
              const rowsOut = operator.stats[ROWS_OUT_STAT_KEY]?.value || 0;

              const extraStats = Object.entries(operator.stats)
                .filter(
                  ([key]) =>
                    ![
                      ROWS_IN_STAT_KEY,
                      ROWS_OUT_STAT_KEY,
                      DURATION_US_STAT_KEY,
                    ].includes(key)
                )
                .map(
                  ([key, stat]) =>
                    `${key.charAt(0).toUpperCase() + key.slice(1)}: ${formatStatValue(stat)}`
                )
                .join(", ");

              return (
                <div
                  key={operatorId}
                  className="grid grid-cols-[50px_60px_100px_200px_120px_120px_100px_1fr] gap-0 items-center min-h-[55px] transition-colors hover:bg-zinc-800/50"
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
                  <div
                    className={`px-3 py-4 text-sm text-zinc-200 truncate border-r border-zinc-700 h-full flex items-center`}
                  >
                    {name}
                  </div>
                  <div className="px-3 py-4 text-right text-sm text-zinc-300 font-mono border-r border-zinc-700 h-full flex items-center justify-end">
                    {name.includes("Scan") ? "-" : rowsIn.toLocaleString()}
                  </div>
                  <div className="px-3 py-4 text-right text-sm text-zinc-300 font-mono border-r border-zinc-700 h-full flex items-center justify-end">
                    {name.includes("Sink") ? "-" : rowsOut.toLocaleString()}
                  </div>
                  <div className="px-3 py-4 text-right text-sm text-zinc-300 font-mono border-r border-zinc-700 h-full flex items-center justify-end">
                    <OperatorDuration operator={operator} />
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
