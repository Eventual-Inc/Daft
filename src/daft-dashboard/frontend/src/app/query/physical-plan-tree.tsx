"use client";

import { useState, useEffect } from "react";
import { main } from "@/lib/utils";
import { ExecutingState, OperatorInfo, PhysicalPlanNode } from "./types";
import {
  getStatusIcon,
  formatStatValue,
  formatDuration,
  ROWS_IN_STAT_KEY,
  ROWS_OUT_STAT_KEY,
  DURATION_US_STAT_KEY,
} from "./stats-utils";
import ProgressTable from "./progress-table";
import TreeLayout from "./tree-layout";
import { getHeatmapStyle, FINISHED_STYLE } from "./tree-colors";

function getCpuSec(operator?: OperatorInfo): number {
  if (!operator) return 0;
  const cpuStat = operator.stats[DURATION_US_STAT_KEY];
  if (cpuStat?.type === "Duration") {
    return cpuStat.value.secs + cpuStat.value.nanos / 1e9;
  }
  return 0;
}

function getCountStat(
  operator: OperatorInfo | undefined,
  key: string,
): number | null {
  const stat = operator?.stats[key];
  if (!stat || stat.type !== "Count") return null;
  return stat.value;
}

/**
 * NodeType variants where (rows_in - rows_out) is a meaningful queue-depth
 * signal — i.e. the operator is a true 1:1 row transform in steady state.
 *
 * Excluded on purpose:
 *   - Reductive: Filter, Sample, Limit (rows_in > rows_out by design,
 *     would otherwise be misread as a stuck queue)
 *   - Amplifying: Explode, Unpivot (rows_in < rows_out)
 *   - Multi-input / variable cardinality: Concat, all join variants
 *   - Blocking sinks (Aggregate, Sort, Write, ...) and sources — handled
 *     by cpu_fraction alone
 */
const QUEUE_SIGNAL_NODE_TYPES = new Set<string>([
  "Project",
  "UDFProject",
  "VLLMProject",
  "AsyncUDFProject",
  "DistributedActorPoolProject",
  "IntoBatches",
  "MonotonicallyIncreasingId",
]);

/**
 * Combined bottleneck signal in [0, 1].
 *
 * Two signals, take whichever is louder:
 *   1. cpu_fraction = cpu_us / max(cpu_us across operators)
 *      — flags CPU-bound work (image_decode, image_resize, sync compute).
 *   2. queue_fraction = (rows_in - rows_out) / rows_in
 *      — flags ops sitting on input they haven't emitted (UDF predict
 *        batching for GPU, async stalls). Only applied to operators in
 *        QUEUE_SIGNAL_NODE_TYPES so that reductive/amplifying ops don't
 *        produce false positives.
 */
function getBottleneckIntensity(
  operator: OperatorInfo | undefined,
  nodeType: string | undefined,
  maxCpuSec: number,
): number {
  if (!operator) return 0;

  const cpuFraction =
    maxCpuSec > 0 ? Math.min(1, getCpuSec(operator) / maxCpuSec) : 0;

  let queueFraction = 0;
  if (nodeType && QUEUE_SIGNAL_NODE_TYPES.has(nodeType)) {
    const rowsIn = getCountStat(operator, ROWS_IN_STAT_KEY);
    const rowsOut = getCountStat(operator, ROWS_OUT_STAT_KEY);
    if (rowsIn != null && rowsOut != null && rowsIn > 0) {
      queueFraction = Math.max(0, (rowsIn - rowsOut) / rowsIn);
    }
  }

  return Math.max(cpuFraction, queueFraction);
}

function useWallClockDuration(operator?: OperatorInfo): string | null {
  const [now, setNow] = useState(() => Date.now() / 1000);
  const isExecuting = operator?.status === "Executing";

  useEffect(() => {
    if (!isExecuting) return;
    const id = setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => clearInterval(id);
  }, [isExecuting]);

  if (!operator?.start_sec) return null;
  const end = operator.end_sec ?? (isExecuting ? now : null);
  if (end == null) return null;
  return formatDuration(Math.max(0, end - operator.start_sec));
}

function PhysicalNodeCard({
  node,
  operator,
  intensity,
}: {
  node: PhysicalPlanNode;
  operator?: OperatorInfo;
  intensity: number;
}) {
  const [expanded, setExpanded] = useState(false);
  const status = operator?.status ?? "Pending";
  const wallClock = useWallClockDuration(operator);
  const cardStyle: React.CSSProperties =
    status === "Finished" ? FINISHED_STYLE : getHeatmapStyle(intensity);

  const rowsIn = operator?.stats[ROWS_IN_STAT_KEY]?.value ?? 0;
  const rowsOut = operator?.stats[ROWS_OUT_STAT_KEY]?.value ?? 0;

  const cpuTimeStat = operator?.stats[DURATION_US_STAT_KEY];
  const extraStats = operator
    ? Object.entries(operator.stats)
        .filter(
          ([key]) =>
            ![ROWS_IN_STAT_KEY, ROWS_OUT_STAT_KEY, DURATION_US_STAT_KEY].includes(key),
        )
        .sort(([a], [b]) => a.localeCompare(b))
    : [];
  const hasExpandable = extraStats.length > 0 || cpuTimeStat;

  return (
    <div
      className="border-2 rounded-lg px-4 py-2.5 cursor-pointer
        hover:brightness-125 transition-all min-w-[180px] max-w-[320px]"
      style={cardStyle}
      onClick={() => setExpanded(!expanded)}
    >
      {/* Header: status icon + name */}
      <div className="flex items-center gap-2">
        {getStatusIcon(status)}
        <span
          className={`${main.className} text-zinc-100 text-sm font-bold tracking-wide truncate`}
        >
          {node.name}
        </span>
        {hasExpandable && (
          <span className="text-zinc-500 text-xs ml-auto">
            {expanded ? "▾" : "▸"}
          </span>
        )}
      </div>

      {/* Rows in / out + wall-clock duration — always visible */}
      {operator && (
        <div className="mt-1.5 flex gap-3 text-xs font-mono text-zinc-400">
          {!node.name.includes("Scan") && (
            <span>
              <span className="text-zinc-500">in:</span> {rowsIn.toLocaleString()}
            </span>
          )}
          {!node.name.includes("Sink") && (
            <span>
              <span className="text-zinc-500">out:</span> {rowsOut.toLocaleString()}
            </span>
          )}
          {wallClock && (
            <span className="text-zinc-400">
              {wallClock}
            </span>
          )}
        </div>
      )}

      {/* Extra stats + CPU time — expandable */}
      {expanded && hasExpandable && (
        <div className="mt-2 pt-2 border-t border-zinc-700/50 space-y-1">
          {cpuTimeStat && (
            <div className="flex justify-between gap-2">
              <span
                className={`${main.className} text-[10px] uppercase tracking-wider text-zinc-500`}
              >
                CPU time
              </span>
              <span className={`${main.className} text-xs text-zinc-300 font-mono`}>
                {formatStatValue(cpuTimeStat)}
              </span>
            </div>
          )}
          {extraStats.map(([key, stat]) => (
            <div key={key} className="flex justify-between gap-2">
              <span
                className={`${main.className} text-[10px] uppercase tracking-wider text-zinc-500`}
              >
                {key}
              </span>
              <span className={`${main.className} text-xs text-zinc-300 font-mono`}>
                {formatStatValue(stat)}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default function PhysicalPlanTree({
  exec_state,
}: {
  exec_state: ExecutingState;
}) {
  const [viewMode, setViewMode] = useState<"tree" | "table" | "json">("tree");

  let plan: PhysicalPlanNode | null = null;
  const planJson = exec_state.exec_info.physical_plan;
  if (planJson) {
    try {
      plan = JSON.parse(planJson);
    } catch {
      // fall through — will show table
    }
  }

  const operators = exec_state.exec_info.operators;
  const maxCpuSec = Math.max(
    0.001,
    ...Object.values(operators).map(getCpuSec),
  );

  return (
    <div className="bg-zinc-900 h-full flex flex-col">
      {/* View toggle */}
      <div className="flex items-center gap-2 px-4 pt-3 pb-2 border-b border-zinc-800">
        {plan && (
          <button
            onClick={() => setViewMode("tree")}
            className={`${main.className} text-xs px-3 py-1 rounded-md transition-colors ${
              viewMode === "tree"
                ? "bg-zinc-700 text-white"
                : "text-zinc-400 hover:text-zinc-200"
            }`}
          >
            Tree View
          </button>
        )}
        <button
          onClick={() => setViewMode("table")}
          className={`${main.className} text-xs px-3 py-1 rounded-md transition-colors ${
            viewMode === "table"
              ? "bg-zinc-700 text-white"
              : "text-zinc-400 hover:text-zinc-200"
          }`}
        >
          Table
        </button>
        {plan && (
          <button
            onClick={() => setViewMode("json")}
            className={`${main.className} text-xs px-3 py-1 rounded-md transition-colors ${
              viewMode === "json"
                ? "bg-zinc-700 text-white"
                : "text-zinc-400 hover:text-zinc-200"
            }`}
          >
            JSON
          </button>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto">
        {viewMode === "tree" && plan ? (
          <div className="relative flex justify-center py-6 px-4 overflow-auto">
            <TreeLayout
              node={plan}
              getChildren={(node) => node.children ?? []}
              renderNode={(node) => {
                const op = operators[node.id];
                const intensity = getBottleneckIntensity(
                  op,
                  node.type,
                  maxCpuSec,
                );
                return (
                  <PhysicalNodeCard
                    node={node}
                    operator={op}
                    intensity={intensity}
                  />
                );
              }}
            />
          </div>
        ) : viewMode === "json" && plan ? (
          <pre
            className={`${main.className} text-sm font-mono text-zinc-300 whitespace-pre-wrap p-4`}
          >
            {JSON.stringify(plan, null, 2)}
          </pre>
        ) : (
          <ProgressTable exec_state={exec_state} />
        )}
      </div>
    </div>
  );
}
