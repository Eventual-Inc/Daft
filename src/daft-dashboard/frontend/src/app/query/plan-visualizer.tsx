"use client";

import { useState } from "react";
import { main } from "@/lib/utils";

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

const categoryColors: Record<string, { bg: string; border: string; text: string }> = {
  Source: { bg: "bg-emerald-950", border: "border-emerald-700", text: "text-emerald-300" },
  Filter: { bg: "bg-amber-950", border: "border-amber-700", text: "text-amber-300" },
  Project: { bg: "bg-sky-950", border: "border-sky-700", text: "text-sky-300" },
  Sort: { bg: "bg-violet-950", border: "border-violet-700", text: "text-violet-300" },
  Join: { bg: "bg-rose-950", border: "border-rose-700", text: "text-rose-300" },
  Aggregate: { bg: "bg-fuchsia-950", border: "border-fuchsia-700", text: "text-fuchsia-300" },
};

const defaultColor = { bg: "bg-zinc-900", border: "border-zinc-600", text: "text-zinc-300" };

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

function PlanTree({ node }: { node: PlanNode }) {
  return (
    <div className="relative flex justify-center py-6 px-4 overflow-auto">
      <TreeNode node={node} />
    </div>
  );
}

function TreeNode({ node, depth = 0 }: { node: PlanNode; depth?: number }) {
  const [expanded, setExpanded] = useState(true);
  const hasChildren = node.children && node.children.length > 0;

  return (
    <div className="flex flex-col items-center min-w-0">
      <NodeCard
        node={node}
        isExpanded={expanded}
        onToggle={() => setExpanded(!expanded)}
      />

      {hasChildren && (
        <div className="flex flex-col items-center">
          {/* Vertical line from parent */}
          <div className="w-px h-6 bg-zinc-600" />

          {node.children.length > 1 ? (
            <div className="flex items-start">
              {node.children.map((child, i) => {
                const isFirst = i === 0;
                const isLast = i === node.children.length - 1;
                return (
                  <div key={i} className="flex flex-col items-center">
                    {/* Horizontal + vertical connector */}
                    <div className="flex w-full h-px">
                      <div
                        className={`flex-1 h-px ${isFirst ? "bg-transparent" : "bg-zinc-600"}`}
                      />
                      <div
                        className={`flex-1 h-px ${isLast ? "bg-transparent" : "bg-zinc-600"}`}
                      />
                    </div>
                    <div className="w-px h-6 bg-zinc-600" />
                    <div className="px-3">
                      <TreeNode node={child} depth={depth + 1} />
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <TreeNode node={node.children[0]} depth={depth + 1} />
          )}
        </div>
      )}
    </div>
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
          <PlanTree node={plan} />
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
