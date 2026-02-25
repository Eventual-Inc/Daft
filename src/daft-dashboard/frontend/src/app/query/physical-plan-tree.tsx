"use client";

import { useState } from "react";
import { main } from "@/lib/utils";
import { ExecutingState, OperatorInfo, PhysicalPlanNode } from "./types";
import {
  getStatusIcon,
  getStatusBorderColor,
  formatStatValue,
  ROWS_IN_STAT_KEY,
  ROWS_OUT_STAT_KEY,
  DURATION_US_STAT_KEY,
} from "./stats-utils";
import ProgressTable from "./progress-table";
import TreeLayout from "./tree-layout";
import { categoryColors, defaultColor } from "./tree-colors";

function getCategoryColor(node: PhysicalPlanNode) {
  // Try node.type first (e.g. "Source", "Join"), then node.category
  return categoryColors[node.type] ?? categoryColors[node.category] ?? defaultColor;
}

function PhysicalNodeCard({
  node,
  operator,
}: {
  node: PhysicalPlanNode;
  operator?: OperatorInfo;
}) {
  const [expanded, setExpanded] = useState(false);
  const catColor = getCategoryColor(node);
  const status = operator?.status ?? "Pending";
  const statusBorder = getStatusBorderColor(status);

  const rowsIn = operator?.stats[ROWS_IN_STAT_KEY]?.value ?? 0;
  const rowsOut = operator?.stats[ROWS_OUT_STAT_KEY]?.value ?? 0;

  const extraStats = operator
    ? Object.entries(operator.stats).filter(
        ([key]) =>
          ![ROWS_IN_STAT_KEY, ROWS_OUT_STAT_KEY, DURATION_US_STAT_KEY].includes(key),
      )
    : [];

  return (
    <div
      className={`${catColor.bg} ${statusBorder} border-2 rounded-lg px-4 py-2.5 cursor-pointer
        hover:brightness-125 transition-all min-w-[180px] max-w-[320px]`}
      onClick={() => setExpanded(!expanded)}
    >
      {/* Header: status icon + name */}
      <div className="flex items-center gap-2">
        {getStatusIcon(status)}
        <span
          className={`${main.className} ${catColor.text} text-sm font-bold tracking-wide truncate`}
        >
          {node.name}
        </span>
        {extraStats.length > 0 && (
          <span className="text-zinc-500 text-xs ml-auto">
            {expanded ? "▾" : "▸"}
          </span>
        )}
      </div>

      {/* Rows in / out — always visible */}
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
        </div>
      )}

      {/* Extra stats — expandable */}
      {expanded && extraStats.length > 0 && (
        <div className="mt-2 pt-2 border-t border-zinc-700/50 space-y-1">
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
              renderNode={(node) => (
                <PhysicalNodeCard node={node} operator={operators[node.id]} />
              )}
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
