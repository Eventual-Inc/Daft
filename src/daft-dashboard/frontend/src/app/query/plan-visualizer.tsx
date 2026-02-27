"use client";

import { useState } from "react";
import { main } from "@/lib/utils";
import TreeLayout from "./tree-layout";
import { categoryColors, defaultColor } from "./tree-colors";

type PlanNode = {
  type: string;
  children: PlanNode[];
  [key: string]: unknown;
};

function parsePlan(planJson: string): PlanNode {
  return JSON.parse(planJson);
}

/** Extract display properties from a node (everything except type and children). */
function getNodeProps(node: PlanNode): [string, unknown][] {
  return Object.entries(node).filter(
    ([key]) => key !== "type" && key !== "children",
  );
}

function getColor(nodeType: string) {
  return categoryColors[nodeType] ?? defaultColor;
}

function formatValue(value: unknown): string {
  if (Array.isArray(value)) {
    return value.map((v) => String(v)).join(", ");
  }
  if (value === null || value === undefined) {
    return "—";
  }
  if (typeof value === "object") {
    return JSON.stringify(value, null, 2);
  }
  return String(value);
}

function NodeCard({
  node,
  isExpanded,
  onToggle,
}: {
  node: PlanNode;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  const color = getColor(node.type);
  const props = getNodeProps(node);

  return (
    <div
      className={`${color.bg} ${color.border} border rounded-lg px-4 py-2.5 cursor-pointer
        hover:brightness-125 transition-all min-w-[160px] max-w-[320px]`}
      onClick={onToggle}
    >
      <div className="flex items-center gap-2">
        <span
          className={`${main.className} ${color.text} text-sm font-bold tracking-wide`}
        >
          {node.type}
        </span>
        {props.length > 0 && (
          <span className="text-zinc-500 text-xs ml-auto">
            {isExpanded ? "▾" : "▸"}
          </span>
        )}
      </div>
      {isExpanded && props.length > 0 && (
        <div className="mt-2 pt-2 border-t border-zinc-700/50 space-y-1">
          {props.map(([key, value]) => (
            <div key={key} className="flex flex-col">
              <span
                className={`${main.className} text-[10px] uppercase tracking-wider text-zinc-500`}
              >
                {key}
              </span>
              <span
                className={`${main.className} text-xs text-zinc-300 break-all`}
              >
                {formatValue(value)}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function ExpandableNodeCard({ node }: { node: PlanNode }) {
  const [expanded, setExpanded] = useState(true);
  return (
    <NodeCard
      node={node}
      isExpanded={expanded}
      onToggle={() => setExpanded(!expanded)}
    />
  );
}

export default function PlanVisualizer({ planJson }: { planJson: string }) {
  const [viewMode, setViewMode] = useState<"tree" | "json">("tree");

  let plan: PlanNode;
  try {
    plan = parsePlan(planJson);
  } catch {
    return (
      <div className="bg-zinc-900 p-4">
        <pre
          className={`${main.className} text-sm font-mono text-zinc-300 whitespace-pre-wrap`}
        >
          {planJson}
        </pre>
      </div>
    );
  }

  return (
    <div className="bg-zinc-900 h-full flex flex-col">
      {/* View toggle */}
      <div className="flex items-center gap-2 px-4 pt-3 pb-2 border-b border-zinc-800">
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
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto">
        {viewMode === "tree" ? (
          <div className="relative flex justify-center py-6 px-4 overflow-auto">
            <TreeLayout
              node={plan}
              getChildren={(node) => node.children ?? []}
              renderNode={(node) => <ExpandableNodeCard node={node} />}
            />
          </div>
        ) : (
          <pre
            className={`${main.className} text-sm font-mono text-zinc-300 whitespace-pre-wrap p-4`}
          >
            {JSON.stringify(plan, null, 2)}
          </pre>
        )}
      </div>
    </div>
  );
}
