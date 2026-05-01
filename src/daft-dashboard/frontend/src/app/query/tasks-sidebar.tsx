"use client";

import { useMemo, useState } from "react";
import { ChevronDown, ChevronRight, PanelRightClose, X } from "lucide-react";
import { main } from "@/lib/utils";
import { ExecutingState, OperatorStatus, TaskInfo } from "./types";
import {
  formatBytes,
  formatCount,
  formatDuration,
  getStatusIcon,
  getStatusColor,
  getStatusText,
} from "./stats-utils";
import {
  buildTaskRows,
  getActiveTasks,
  PlanChainNode,
  taskDisplayStatus,
  TaskTypeRow,
  TOP_K_RUNNING,
} from "./tasks-grouping";

/**
 * Tasks sidebar — a collapsible panel docked inside the Execution tab that
 * shows Flotilla task "types" (groups of tasks sharing a `node_ids` chain).
 * Rows are expandable to individual task level.
 *
 * Task data comes from `exec_info.task_store`, which contains per-group
 * aggregate summaries and a bounded set of retained individual tasks.
 *
 * The filter is by node-id membership: a row is shown if `nodeFilter` is null
 * or if `row.node_ids.includes(nodeFilter)`. Hovering a row notifies the page
 * via `onHoverNodes` so the physical plan tree can preview-highlight all
 * nodes in the chain.
 */
export default function TasksSidebar({
  exec_state,
  nodeFilter,
  onClearFilter,
  onSelectNode,
  onHoverNodes,
  onClose,
}: {
  exec_state: ExecutingState;
  /** If set, only show rows whose `node_ids` contains this id. */
  nodeFilter: number | null;
  /** Called when the user clears the node filter. */
  onClearFilter: () => void;
  /** Called when the user clicks a chip — narrows the filter to that node id. */
  onSelectNode: (nodeId: number) => void;
  /** Called when the user hovers a row (a Set of node ids), or null on leave. */
  onHoverNodes: (ids: ReadonlySet<number> | null) => void;
  /** Called when the user closes the sidebar. */
  onClose: () => void;
}) {
  const queryActive = exec_state.status === "Executing";
  const { operators, task_store } = exec_state.exec_info;

  const allRows = useMemo(() => buildTaskRows(task_store, operators), [task_store, operators]);

  const rows = useMemo(
    () =>
      nodeFilter == null
        ? allRows
        : allRows.filter((r) => r.node_ids.includes(nodeFilter)),
    [allRows, nodeFilter],
  );

  // Active tasks for the "top" section.
  const allActiveTasks = useMemo(() => getActiveTasks(task_store), [task_store]);
  const filteredActiveTasks = useMemo(
    () =>
      nodeFilter == null
        ? allActiveTasks
        : allActiveTasks.filter((t) => t.node_ids.includes(nodeFilter)),
    [allActiveTasks, nodeFilter],
  );
  const activeTasks = filteredActiveTasks.slice(0, TOP_K_RUNNING);
  const totalRunning = filteredActiveTasks.length;

  // Summary counts from group aggregates (always accurate).
  const totalTasks = rows.reduce((a, r) => a + r.task_count, 0);
  const totalExecuting = rows.reduce((a, r) => a + r.status_counts.Executing, 0);
  const totalFinished = rows.reduce((a, r) => a + r.status_counts.Finished, 0);
  const totalFailed = rows.reduce((a, r) => a + r.status_counts.Failed, 0);

  const filteredNodeName =
    nodeFilter != null
      ? (operators[nodeFilter]?.node_info.name ?? `Node ${nodeFilter}`)
      : null;

  return (
    <div className="bg-zinc-900 h-full flex flex-col">
      <div className="flex items-center justify-between gap-3 px-4 pt-3 pb-2 border-b border-zinc-800">
        <div className={`${main.className} text-xs text-zinc-400 flex items-center gap-2 min-w-0`}>
          <span className={`${main.className} text-sm font-bold text-zinc-200`}>
            Tasks
          </span>
          <span>
            {totalExecuting > 0 && <span className="text-emerald-400">{totalExecuting} running</span>}
            {totalExecuting > 0 && totalFinished > 0 && " · "}
            {totalFinished > 0 && <span>{totalFinished} finished</span>}
            {totalFailed > 0 && " · "}
            {totalFailed > 0 && <span className="text-red-400">{totalFailed} failed</span>}
            {totalTasks === 0 && "0 tasks"}
          </span>
          {nodeFilter != null && (
            <span className="flex items-center gap-1 px-2 py-0.5 rounded-md bg-fuchsia-950/50 border border-fuchsia-800 text-fuchsia-200 truncate">
              <span className="truncate">
                contains: {filteredNodeName} (#{nodeFilter})
              </span>
              <button
                onClick={onClearFilter}
                className="hover:text-white flex-shrink-0"
                aria-label="Clear node filter"
              >
                <X size={12} />
              </button>
            </span>
          )}
        </div>
        <button
          onClick={onClose}
          className="flex items-center gap-1 px-2 py-1 rounded-md text-xs text-zinc-400
            hover:text-white hover:bg-zinc-800 transition-colors flex-shrink-0"
          title="Close tasks sidebar"
          aria-label="Close tasks sidebar"
        >
          <PanelRightClose size={14} />
        </button>
      </div>

      <div className="flex-1 overflow-auto">
        {rows.length === 0 && activeTasks.length === 0 ? (
          <div className="p-8 text-center">
            <p className={`${main.className} text-zinc-400`}>
              {nodeFilter != null
                ? "No tasks for this filter."
                : allRows.length === 0
                  ? "No tasks reported yet."
                  : "No tasks match."}
            </p>
          </div>
        ) : (
          <>
            <div
              className="overflow-hidden transition-all duration-500 ease-in-out"
              style={{
                maxHeight: queryActive ? "600px" : "0px",
                opacity: queryActive ? 1 : 0,
              }}
            >
              <RunningTasksSection
                tasks={activeTasks}
                totalRunning={totalRunning}
                onHoverNodes={onHoverNodes}
              />
            </div>
            <TaskTypeTable
              rows={rows}
              onSelectNode={onSelectNode}
              onHoverNodes={onHoverNodes}
            />
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Running tasks "top" section.
// ---------------------------------------------------------------------------
// Columns: Local Plan | Rows in | Rows out | Bytes in | Bytes out | CPU
const RUNNING_GRID_COLS =
  "grid-cols-[minmax(200px,2fr)_70px_70px_80px_80px_90px]";

function RunningTasksSection({
  tasks,
  totalRunning,
  onHoverNodes,
}: {
  tasks: TaskInfo[];
  totalRunning: number;
  onHoverNodes: (ids: ReadonlySet<number> | null) => void;
}) {
  return (
    <div className="border-b border-zinc-700">
      <div className="px-4 py-2 bg-emerald-950/30 border-b border-zinc-800">
        <span className={`${main.className} text-xs font-bold text-emerald-300 uppercase tracking-wider`}>
          Running ({totalRunning})
        </span>
      </div>
      <div className="min-w-[600px]">
        <div
          className={`grid ${RUNNING_GRID_COLS} gap-0 items-center min-h-[32px] bg-zinc-800/50 border-b border-zinc-800`}
        >
          <RunningHeader align="left">Local Plan</RunningHeader>
          <RunningHeader align="right">Rows in</RunningHeader>
          <RunningHeader align="right">Rows out</RunningHeader>
          <RunningHeader align="right">Bytes in</RunningHeader>
          <RunningHeader align="right">Bytes out</RunningHeader>
          <RunningHeader align="right">CPU</RunningHeader>
        </div>
        {Array.from({ length: TOP_K_RUNNING }, (_, i) => {
          const task = tasks[i];
          return task ? (
            <RunningTaskRow
              key={task.task_id}
              task={task}
              onHoverNodes={onHoverNodes}
            />
          ) : (
            <div key={`empty-${i}`} className={`grid ${RUNNING_GRID_COLS} gap-0 items-center min-h-[36px]`} />
          );
        })}
        {totalRunning > tasks.length && (
          <div className={`${main.className} px-4 py-1.5 text-xs text-zinc-500 italic border-t border-zinc-800/50`}>
            Showing {tasks.length} of {totalRunning} running tasks (highest busy time first)
          </div>
        )}
      </div>
    </div>
  );
}

function RunningHeader({
  children,
  align = "left",
}: {
  children: React.ReactNode;
  align?: "left" | "right";
}) {
  const justify = align === "right" ? "justify-end" : "justify-start";
  return (
    <div
      className={`px-3 py-1.5 text-[10px] uppercase tracking-wider text-zinc-500 font-mono flex items-center ${justify}`}
    >
      {children}
    </div>
  );
}

function RunningTaskRow({
  task,
  onHoverNodes,
}: {
  task: TaskInfo;
  onHoverNodes: (ids: ReadonlySet<number> | null) => void;
}) {
  // CPU time (busy time, sum of operator DURATION_KEY across this task's
  // pipeline) rather than wall-clock since submit. Refreshed mid-flight from
  // TaskStatsUpdate events; re-renders driven by taskStore prop changes
  // upstream, so no per-second timer needed. Trailing ellipsis hints this is
  // a running snapshot, not final. The rows/bytes I/O counters use is_task_root /
  // is_task_leaf filtering so they reflect only the task's external traffic.
  const cpu = formatDuration(task.cpu_us / 1_000_000) + "\u2026";
  const pipeline = task.name
    ? task.name.includes("->") ? task.name.split("->") : [task.name]
    : [`Node ${task.last_node_id}`];
  const ids = task.node_ids.length > 0 ? task.node_ids : [task.last_node_id];

  return (
    <div
      className={`grid ${RUNNING_GRID_COLS} gap-0 items-center min-h-[36px] hover:bg-zinc-800/40 transition-colors`}
      onMouseEnter={() => onHoverNodes(new Set(ids))}
      onMouseLeave={() => onHoverNodes(null)}
    >
      <div className="px-3">
        <PipelineChips pipeline={pipeline} />
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatCount(task.rows_in)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatCount(task.rows_out)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatBytes(task.bytes_in)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatBytes(task.bytes_out)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-emerald-300 font-mono`}>
        {cpu}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Table of task-type rows.
// ---------------------------------------------------------------------------
// Columns: chevron | Local Plan | Distributed Plan | Tasks | Status | Rows in | Rows out | Bytes in | Bytes out | CPU
const GRID_COLS =
  "grid-cols-[32px_minmax(220px,2fr)_minmax(220px,2fr)_90px_130px_80px_80px_90px_90px_100px]";

function TaskTypeTable({
  rows,
  onSelectNode,
  onHoverNodes,
}: {
  rows: TaskTypeRow[];
  onSelectNode: (nodeId: number) => void;
  onHoverNodes: (ids: ReadonlySet<number> | null) => void;
}) {
  return (
    <div className="min-w-[1200px]">
      {/* Header */}
      <div
        className={`bg-zinc-800 grid ${GRID_COLS} gap-0 items-center min-h-[48px] border-b border-zinc-700 sticky top-0 z-10`}
      >
        <HeaderCell />
        <HeaderCell align="left">Local Plan</HeaderCell>
        <HeaderCell align="left">Distributed Plan</HeaderCell>
        <HeaderCell align="right">Tasks</HeaderCell>
        <HeaderCell align="left">Status</HeaderCell>
        <HeaderCell align="right">Rows in</HeaderCell>
        <HeaderCell align="right">Rows out</HeaderCell>
        <HeaderCell align="right">Bytes in</HeaderCell>
        <HeaderCell align="right">Bytes out</HeaderCell>
        <HeaderCell align="right" last>
          CPU
        </HeaderCell>
      </div>

      {/* Body */}
      <div className="divide-y divide-zinc-800">
        {rows.map((row) => (
          <TaskTypeGroupRow
            key={row.key}
            row={row}
            onSelectNode={onSelectNode}
            onHoverNodes={onHoverNodes}
          />
        ))}
      </div>
    </div>
  );
}

function HeaderCell({
  children,
  align = "center",
  last = false,
}: {
  children?: React.ReactNode;
  align?: "left" | "right" | "center";
  last?: boolean;
}) {
  const justify =
    align === "left"
      ? "justify-start"
      : align === "right"
        ? "justify-end"
        : "justify-center";
  const textAlign =
    align === "left" ? "text-left" : align === "right" ? "text-right" : "text-center";
  return (
    <div
      className={`px-3 py-3 text-xs font-bold text-white font-mono h-full flex items-center ${justify} ${textAlign} ${last ? "" : "border-r border-zinc-700"}`}
    >
      {children}
    </div>
  );
}

function TaskTypeGroupRow({
  row,
  onSelectNode,
  onHoverNodes,
}: {
  row: TaskTypeRow;
  onSelectNode: (nodeId: number) => void;
  onHoverNodes: (ids: ReadonlySet<number> | null) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const hoverSet = useMemo(() => new Set(row.node_ids), [row.node_ids]);

  return (
    <>
      <div
        className={`grid ${GRID_COLS} gap-0 items-center min-h-[48px] cursor-pointer hover:bg-zinc-800/50 transition-colors`}
        onClick={() => setExpanded((e) => !e)}
        onMouseEnter={() => onHoverNodes(hoverSet)}
        onMouseLeave={() => onHoverNodes(null)}
      >
        <Cell align="center">
          {expanded ? (
            <ChevronDown size={14} className="text-zinc-400" />
          ) : (
            <ChevronRight size={14} className="text-zinc-400" />
          )}
        </Cell>
        <Cell align="left">
          <PipelineChips pipeline={row.pipeline} />
        </Cell>
        <Cell align="left">
          <ClickablePipelineChips
            chain={row.distributed_plan}
            onClickNode={onSelectNode}
          />
        </Cell>
        <Cell align="right">
          <span className={`${main.className} text-sm text-zinc-200 font-mono`}>
            {row.task_count}
          </span>
        </Cell>
        <Cell align="left">
          <StatusSummary counts={row.status_counts} />
        </Cell>
        <Cell align="right">
          <Mono>{formatCount(row.total_rows_in)}</Mono>
        </Cell>
        <Cell align="right">
          <Mono>{formatCount(row.total_rows_out)}</Mono>
        </Cell>
        <Cell align="right">
          <Mono>{formatBytes(row.total_bytes_in)}</Mono>
        </Cell>
        <Cell align="right">
          <Mono>{formatBytes(row.total_bytes_out)}</Mono>
        </Cell>
        <Cell align="right" last>
          <Mono>{formatDuration(row.total_cpu_sec)}</Mono>
        </Cell>
      </div>

      {expanded && (
        <ExpandedTaskList
          tasks={row.tasks}
          taskCount={row.task_count}
          retainedTaskCount={row.retained_task_count}
        />
      )}
    </>
  );
}

function Cell({
  children,
  align = "center",
  last = false,
}: {
  children?: React.ReactNode;
  align?: "left" | "right" | "center";
  last?: boolean;
}) {
  const justify =
    align === "left"
      ? "justify-start"
      : align === "right"
        ? "justify-end"
        : "justify-center";
  return (
    <div
      className={`px-3 py-3 h-full flex items-center ${justify} min-w-0 ${last ? "" : "border-r border-zinc-800"}`}
    >
      {children}
    </div>
  );
}

function Mono({ children }: { children: React.ReactNode }) {
  return (
    <span className={`${main.className} text-sm text-zinc-300 font-mono`}>{children}</span>
  );
}

/** Renders a pipeline as chips connected by arrows: [Read] → [Project] */
function PipelineChips({ pipeline }: { pipeline: string[] }) {
  return (
    <div className="flex items-center gap-1 flex-wrap min-w-0">
      {pipeline.map((name, i) => (
        <span key={i} className="flex items-center gap-1 min-w-0">
          <span
            className={`${main.className} px-2 py-0.5 rounded-md bg-zinc-800 border border-zinc-700 text-xs text-zinc-200 font-mono truncate`}
            title={name}
          >
            {name}
          </span>
          {i < pipeline.length - 1 && (
            <span className="text-zinc-500 text-xs">→</span>
          )}
        </span>
      ))}
    </div>
  );
}

/**
 * Like {@link PipelineChips} but each chip is a clickable button. Clicking a
 * chip filters the sidebar to tasks containing that node id. We stop click
 * propagation so the row's expand/collapse handler doesn't also fire.
 */
function ClickablePipelineChips({
  chain,
  onClickNode,
}: {
  chain: PlanChainNode[];
  onClickNode: (id: number) => void;
}) {
  return (
    <div className="flex items-center gap-1 flex-wrap min-w-0">
      {chain.map((node, i) => (
        <span key={`${node.id}-${i}`} className="flex items-center gap-1 min-w-0">
          <button
            onClick={(e) => {
              e.stopPropagation();
              onClickNode(node.id);
            }}
            className={`${main.className} px-2 py-0.5 rounded-md bg-zinc-800 border border-zinc-700 text-xs text-sky-300 hover:text-sky-200 hover:bg-zinc-700 hover:border-sky-700 font-mono truncate transition-colors`}
            title={`${node.name} #${node.id} — click to filter`}
          >
            {node.name}
          </button>
          {i < chain.length - 1 && (
            <span className="text-zinc-500 text-xs">→</span>
          )}
        </span>
      ))}
    </div>
  );
}

function StatusSummary({
  counts,
}: {
  counts: Record<OperatorStatus, number>;
}) {
  const entries: [OperatorStatus, number][] = [
    ["Executing", counts.Executing],
    ["Finished", counts.Finished],
    ["Pending", counts.Pending],
    ["Failed", counts.Failed],
  ];
  const visible = entries.filter(([, n]) => n > 0);
  return (
    <div className="flex items-center gap-2 flex-wrap">
      {visible.map(([s, n]) => (
        <span key={s} className={`${main.className} text-xs font-mono ${getStatusColor(s)}`}>
          {n} {getStatusText(s).toLowerCase()}
        </span>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Expanded task-level sub-table.
// ---------------------------------------------------------------------------
// Per-task rows/bytes I/O reflect only the task's external traffic. Internal
// flow between fused operators is filtered out by `is_task_root` /
// `is_task_leaf` on each StatSnapshot's NodeInfo before the dashboard
// aggregator sums it up.
// Columns: status | Task ID | Worker | Status | Rows in | Rows out | Bytes in | Bytes out | Duration | CPU
const SUB_GRID_COLS =
  "grid-cols-[32px_80px_minmax(140px,1.5fr)_110px_70px_70px_80px_80px_100px_100px]";

function ExpandedTaskList({
  tasks,
  taskCount,
  retainedTaskCount,
}: {
  tasks: TaskInfo[];
  /** Total number of tasks in this group (from server summary). */
  taskCount: number;
  /** Number of retained individual tasks for this group. */
  retainedTaskCount: number;
}) {
  return (
    <div className="bg-zinc-950/40 border-l-2 border-fuchsia-900/70 pl-0">
      {/* sub-header */}
      <div
        className={`grid ${SUB_GRID_COLS} gap-0 items-center min-h-[36px] bg-zinc-900/60 border-b border-zinc-800`}
      >
        <SubHeader />
        <SubHeader align="left">Task ID</SubHeader>
        <SubHeader align="left">Worker</SubHeader>
        <SubHeader align="left">Status</SubHeader>
        <SubHeader align="right">Rows in</SubHeader>
        <SubHeader align="right">Rows out</SubHeader>
        <SubHeader align="right">Bytes in</SubHeader>
        <SubHeader align="right">Bytes out</SubHeader>
        <SubHeader align="right">Duration</SubHeader>
        <SubHeader align="right">CPU</SubHeader>
      </div>
      {[...tasks]
        .sort((a, b) => a.task_id - b.task_id)
        .map((t) => (
          <TaskSubRow key={t.task_id} task={t} />
        ))}
      {retainedTaskCount < taskCount && (
        <div className={`${main.className} px-6 py-2 text-xs text-zinc-500 italic border-t border-zinc-800/50`}>
          Showing {retainedTaskCount} of {taskCount.toLocaleString()} tasks
          (highest busy time, plus active and failed)
        </div>
      )}
    </div>
  );
}

function SubHeader({
  children,
  align = "center",
}: {
  children?: React.ReactNode;
  align?: "left" | "right" | "center";
}) {
  const justify =
    align === "left"
      ? "justify-start"
      : align === "right"
        ? "justify-end"
        : "justify-center";
  return (
    <div
      className={`px-3 py-2 text-[10px] uppercase tracking-wider text-zinc-500 font-mono h-full flex items-center ${justify}`}
    >
      {children}
    </div>
  );
}

function TaskSubRow({ task }: { task: TaskInfo }) {
  const displayStatus = taskDisplayStatus(task);
  const duration =
    task.end_sec != null
      ? formatDuration(task.end_sec - task.submit_sec)
      : displayStatus === "Executing"
        ? formatDuration(Date.now() / 1000 - task.submit_sec) + "…"
        : "—";
  const cpuSec = task.cpu_us / 1_000_000;

  return (
    <div
      className={`grid ${SUB_GRID_COLS} gap-0 items-center min-h-[36px] hover:bg-zinc-800/40 transition-colors`}
    >
      <div className="flex items-center justify-center">
        {getStatusIcon(displayStatus)}
      </div>
      <div className={`${main.className} px-3 text-xs text-zinc-400 font-mono`}>
        {task.task_id}
      </div>
      <div
        className={`${main.className} px-3 text-xs text-zinc-300 font-mono truncate`}
        title={task.worker_id ?? ""}
      >
        {task.worker_id ?? "—"}
      </div>
      <div className={`${main.className} px-3 text-xs font-mono ${getStatusColor(displayStatus)}`}>
        {getStatusText(displayStatus)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatCount(task.rows_in)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatCount(task.rows_out)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatBytes(task.bytes_in)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatBytes(task.bytes_out)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {duration}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatDuration(cpuSec)}
      </div>
    </div>
  );
}
