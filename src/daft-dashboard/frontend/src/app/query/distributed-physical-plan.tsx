"use client";

import { useMemo, useState } from "react";
import { main } from "@/lib/utils";
import {
  DistributedPhysicalPlan,
  DistributedPhysicalPlanEntry,
  DistributedPhysicalPlanInputSummary,
  PhysicalPlanNode,
} from "./types";

/**
 * Parse the `exec_info.distributed_physical_plan` JSON string. Returns null if
 * the field is missing or malformed (which is the common case while a query is
 * still running — the plan is only posted at exec_end).
 */
export function parseDistributedPhysicalPlan(
  raw: string | undefined,
): DistributedPhysicalPlan | null {
  if (!raw) return null;
  try {
    return JSON.parse(raw) as DistributedPhysicalPlan;
  } catch {
    return null;
  }
}

/**
 * Index of pipeline node id -> entries whose `node_chain` ends at that node.
 * Each pipeline node's `produce_tasks()` is the terminal producer of the
 * local plan, so this mapping is the natural linkage between the distributed
 * pipeline view (left) and the local-plan panel (right).
 */
export function buildEntriesByTerminalNode(
  plan: DistributedPhysicalPlan | null,
): Map<number, DistributedPhysicalPlanEntry[]> {
  const out = new Map<number, DistributedPhysicalPlanEntry[]>();
  if (!plan) return out;
  for (const entry of plan.entries) {
    const terminal = entry.node_chain[entry.node_chain.length - 1];
    if (terminal == null) continue;
    const bucket = out.get(terminal) ?? [];
    bucket.push(entry);
    out.set(terminal, bucket);
  }
  return out;
}

/** Build an id -> name map over the pipeline plan tree for display. */
function buildNodeNameMap(
  plan: PhysicalPlanNode | null,
): Map<number, string> {
  const out = new Map<number, string>();
  if (!plan) return out;
  const stack: PhysicalPlanNode[] = [plan];
  while (stack.length > 0) {
    const n = stack.pop()!;
    out.set(n.id, n.name);
    for (const c of n.children ?? []) stack.push(c);
  }
  return out;
}

function formatInputSummary(
  summary: DistributedPhysicalPlanInputSummary,
): string {
  switch (summary.kind) {
    case "scan_tasks":
      return `${summary.count} scan tasks`;
    case "glob_paths":
      return `${summary.count} glob paths`;
    case "flight_shuffle":
      return `${summary.count} flight shuffle reads`;
    case "in_memory":
      return `${summary.count} in-memory partitions`;
  }
}

function LocalPlanTree({ node }: { node: unknown }) {
  // LocalPhysicalPlan serializes as a tagged enum: each variant is an object
  // with a single key (the variant name) -> payload. Walk the structure
  // recursively, treating any nested object/array similarly. We don't try to
  // be type-aware — the JSON is the source of truth.
  return <LocalPlanValue value={node} depth={0} />;
}

function LocalPlanValue({ value, depth }: { value: unknown; depth: number }) {
  const [open, setOpen] = useState(depth < 2);

  if (value === null || value === undefined) {
    return <span className="text-zinc-500">null</span>;
  }
  if (typeof value === "string") {
    return <span className="text-emerald-300">&quot;{value}&quot;</span>;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return <span className="text-amber-300">{String(value)}</span>;
  }
  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="text-zinc-500">[]</span>;
    return (
      <div className="ml-4 border-l border-zinc-800 pl-2">
        {value.map((v, i) => (
          <div key={i} className="flex gap-2">
            <span className="text-zinc-600 text-xs">[{i}]</span>
            <LocalPlanValue value={v} depth={depth + 1} />
          </div>
        ))}
      </div>
    );
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 0) return <span className="text-zinc-500">{"{}"}</span>;
    // A common case: a single-key variant wrapper. Collapse these inline.
    const isVariant = entries.length === 1;
    return (
      <div className={depth === 0 ? "" : "ml-2"}>
        <button
          type="button"
          onClick={() => setOpen(!open)}
          className={`${main.className} text-zinc-400 text-xs hover:text-zinc-200`}
        >
          {open ? "▾" : "▸"} {isVariant ? entries[0][0] : "{}"}
        </button>
        {open && (
          <div className="ml-4 border-l border-zinc-800 pl-2 mt-0.5">
            {entries.map(([k, v]) => (
              <div key={k}>
                <span className="text-sky-300 text-xs">{k}</span>
                <span className="text-zinc-600 text-xs mx-1">:</span>
                <LocalPlanValue value={v} depth={depth + 1} />
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }
  return <span className="text-zinc-300">{String(value)}</span>;
}

function NodeChain({
  chain,
  nodeNames,
  highlightedId,
  onHover,
}: {
  chain: number[];
  nodeNames: Map<number, string>;
  highlightedId: number | null;
  onHover: (id: number | null) => void;
}) {
  return (
    <div className="flex flex-wrap items-center gap-1 text-xs">
      {chain.map((id, idx) => {
        const name = nodeNames.get(id) ?? `#${id}`;
        const isTerminal = idx === chain.length - 1;
        const isHighlighted = id === highlightedId;
        return (
          <span key={`${idx}-${id}`} className="flex items-center gap-1">
            <button
              type="button"
              onMouseEnter={() => onHover(id)}
              onMouseLeave={() => onHover(null)}
              className={`rounded px-1.5 py-0.5 border transition-colors ${
                isHighlighted
                  ? "border-sky-400 bg-sky-500/20 text-sky-200"
                  : isTerminal
                  ? "border-zinc-600 bg-zinc-800 text-zinc-200"
                  : "border-zinc-700 bg-zinc-900 text-zinc-400"
              }`}
            >
              {name}{" "}
              <span className="text-zinc-500 text-[10px]">#{id}</span>
            </button>
            {!isTerminal && <span className="text-zinc-600">→</span>}
          </span>
        );
      })}
    </div>
  );
}

function EntryCard({
  entry,
  nodeNames,
  highlightedNodeId,
  onHoverNode,
}: {
  entry: DistributedPhysicalPlanEntry;
  nodeNames: Map<number, string>;
  highlightedNodeId: number | null;
  onHoverNode: (id: number | null) => void;
}) {
  const [open, setOpen] = useState(false);
  const inputs = Object.entries(entry.inputs);
  const psets = Object.entries(entry.psets);

  return (
    <div
      className={`rounded-lg border border-zinc-800 bg-zinc-950 overflow-hidden`}
    >
      <button
        type="button"
        className="w-full flex items-center gap-3 px-3 py-2 hover:bg-zinc-900 text-left"
        onClick={() => setOpen(!open)}
      >
        <span className="text-zinc-500 text-xs">{open ? "▾" : "▸"}</span>
        <span
          className={`${main.className} inline-flex items-center gap-1 rounded-full bg-zinc-800 px-2 py-0.5 text-xs text-zinc-200`}
        >
          {entry.count} task{entry.count === 1 ? "" : "s"}
        </span>
        <div className="flex-1 min-w-0">
          <NodeChain
            chain={entry.node_chain}
            nodeNames={nodeNames}
            highlightedId={highlightedNodeId}
            onHover={onHoverNode}
          />
        </div>
        <span className="text-zinc-600 text-[10px] font-mono">
          fp {entry.plan_fingerprint}
        </span>
      </button>
      {open && (
        <div className="px-4 py-3 border-t border-zinc-800 bg-zinc-900/40 space-y-3">
          {(inputs.length > 0 || psets.length > 0) && (
            <div>
              <div
                className={`${main.className} text-[10px] uppercase tracking-wider text-zinc-500 mb-1`}
              >
                Inputs
              </div>
              <div className="flex flex-wrap gap-2 text-xs">
                {inputs.map(([src, summary]) => (
                  <span
                    key={`in-${src}`}
                    className="rounded bg-zinc-800 px-2 py-0.5 text-zinc-300"
                  >
                    <span className="text-zinc-500">src {src}:</span>{" "}
                    {formatInputSummary(summary)}
                  </span>
                ))}
                {psets.map(([src, count]) => (
                  <span
                    key={`ps-${src}`}
                    className="rounded bg-zinc-800 px-2 py-0.5 text-zinc-300"
                  >
                    <span className="text-zinc-500">pset {src}:</span> {count}{" "}
                    partitions
                  </span>
                ))}
              </div>
            </div>
          )}
          <div>
            <div
              className={`${main.className} text-[10px] uppercase tracking-wider text-zinc-500 mb-1`}
            >
              Local plan
            </div>
            <div className="font-mono text-xs text-zinc-300 leading-relaxed">
              <LocalPlanTree node={entry.local_plan} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default function DistributedPhysicalPlanPanel({
  plan,
  pipelineRoot,
  selectedNodeId,
  onHoverNode,
  highlightedNodeId,
}: {
  plan: DistributedPhysicalPlan;
  pipelineRoot: PhysicalPlanNode | null;
  selectedNodeId: number | null;
  onHoverNode: (id: number | null) => void;
  highlightedNodeId: number | null;
}) {
  const nodeNames = useMemo(
    () => buildNodeNameMap(pipelineRoot),
    [pipelineRoot],
  );

  const visibleEntries = useMemo(() => {
    if (selectedNodeId == null) return plan.entries;
    return plan.entries.filter((e) => e.node_chain.includes(selectedNodeId));
  }, [plan.entries, selectedNodeId]);

  const totalTasks = plan.entries.reduce((sum, e) => sum + e.count, 0);
  const shownTasks = visibleEntries.reduce((sum, e) => sum + e.count, 0);

  return (
    <div className="p-4 bg-zinc-950 flex flex-col gap-3 h-full overflow-auto">
      <div className="flex items-center gap-3">
        <h3
          className={`${main.className} text-sm font-bold text-zinc-200 tracking-wide`}
        >
          Distributed physical plan
        </h3>
        <span className="text-xs text-zinc-500">
          {plan.entries.length} plan shape{plan.entries.length === 1 ? "" : "s"} ·{" "}
          {shownTasks}
          {selectedNodeId != null ? ` / ${totalTasks}` : ""} task
          {totalTasks === 1 ? "" : "s"}
        </span>
        {selectedNodeId != null && (
          <span className="text-xs text-sky-300">
            filtered to pipeline node {nodeNames.get(selectedNodeId) ?? "?"} #
            {selectedNodeId}
          </span>
        )}
      </div>
      <p className={`${main.className} text-xs text-zinc-500 leading-relaxed`}>
        These are the local physical plans each distributed pipeline node
        actually produced during execution — the authoritative physical plan
        for this Flotilla query. Click a pipeline node in the Execution view
        to filter.
      </p>
      <div className="flex flex-col gap-2">
        {visibleEntries.length === 0 ? (
          <p className={`${main.className} text-xs text-zinc-500`}>
            No local plans recorded
            {selectedNodeId != null ? " for this pipeline node" : ""}.
          </p>
        ) : (
          visibleEntries.map((entry) => (
            <EntryCard
              key={`${entry.plan_fingerprint}-${entry.node_chain.join(",")}`}
              entry={entry}
              nodeNames={nodeNames}
              highlightedNodeId={highlightedNodeId}
              onHoverNode={onHoverNode}
            />
          ))
        )}
      </div>
    </div>
  );
}
