"use client";

import { useEffect, useMemo, useState } from "react";
import { ChevronDown, ChevronRight, PanelRightClose, X } from "lucide-react";
import { main } from "@/lib/utils";
import { ExecutingState, OperatorStatus, TaskInfo } from "./types";
import {
  formatBytes,
  formatDuration,
  getStatusIcon,
  getStatusColor,
  getStatusText,
} from "./stats-utils";
import {
  buildTaskRows,
  getActiveTasks,
  taskDisplayStatus,
  TaskTypeRow,
  TOP_K_RUNNING,
} from "./tasks-grouping";

/**
 * Tasks sidebar — a collapsible panel docked inside the Execution tab that
 * shows Flotilla task "types" (fused pipelines grouped by origin distributed-
 * plan node). Rows are expandable to individual task level.
 *
 * Task data comes from `exec_info.task_store`, which contains per-group
 * aggregate summaries and a bounded set of retained individual tasks.
 */
export default function TasksSidebar({
  exec_state,
  originFilter,
  onClearFilter,
  onSelectOrigin,
  onClose,
}: {
  exec_state: ExecutingState;
  /** If set, only show rows whose origin_node_id matches. */
  originFilter: number | null;
  /** Called when the user clears the origin filter. */
  onClearFilter: () => void;
  /** Called when the user clicks an origin node name — highlights the plan node. */
  onSelectOrigin: (nodeId: number) => void;
  /** Called when the user closes the sidebar. */
  onClose: () => void;
}) {
  const { operators, task_store } = exec_state.exec_info;

  const allRows = useMemo(() => buildTaskRows(task_store, operators), [task_store, operators]);

  const rows = useMemo(
    () =>
      originFilter == null
        ? allRows
        : allRows.filter((r) => r.origin_node_id === originFilter),
    [allRows, originFilter],
  );

  // Active tasks for the "top" section.
  const allActiveTasks = useMemo(() => getActiveTasks(task_store), [task_store]);
  const filteredActiveTasks = useMemo(
    () =>
      originFilter == null
        ? allActiveTasks
        : allActiveTasks.filter((t) => t.origin_node_id === originFilter),
    [allActiveTasks, originFilter],
  );
  const activeTasks = filteredActiveTasks.slice(0, TOP_K_RUNNING);
  const totalRunning = filteredActiveTasks.length;

  // Summary counts from group aggregates (always accurate).
  const totalTasks = rows.reduce((a, r) => a + r.task_count, 0);
  const totalExecuting = rows.reduce((a, r) => a + r.status_counts.Executing, 0);
  const totalFinished = rows.reduce((a, r) => a + r.status_counts.Finished, 0);
  const totalFailed = rows.reduce((a, r) => a + r.status_counts.Failed, 0);

  const filteredOriginName =
    originFilter != null
      ? (operators[originFilter]?.node_info.name ?? `Node ${originFilter}`)
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
          {originFilter != null && (
            <span className="flex items-center gap-1 px-2 py-0.5 rounded-md bg-fuchsia-950/50 border border-fuchsia-800 text-fuchsia-200 truncate">
              <span className="truncate">
                origin: {filteredOriginName} (#{originFilter})
              </span>
              <button
                onClick={onClearFilter}
                className="hover:text-white flex-shrink-0"
                aria-label="Clear origin filter"
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
              {originFilter != null
                ? "No tasks for this filter."
                : allRows.length === 0
                  ? "No tasks reported yet."
                  : "No tasks match."}
            </p>
          </div>
        ) : (
          <>
            <RunningTasksSection
              tasks={activeTasks}
              totalRunning={totalRunning}
            />
            <TaskTypeTable rows={rows} onSelectOrigin={onSelectOrigin} />
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Running tasks "top" section.
// ---------------------------------------------------------------------------
const RUNNING_GRID_COLS =
  "grid-cols-[minmax(200px,2fr)_90px_100px_100px_100px]";

function RunningTasksSection({
  tasks,
  totalRunning,
}: {
  tasks: TaskInfo[];
  totalRunning: number;
}) {
  const [now, setNow] = useState(() => Date.now() / 1000);
  const hasRunning = tasks.length > 0;

  useEffect(() => {
    if (!hasRunning) return;
    const id = setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => clearInterval(id);
  }, [hasRunning]);

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
          <RunningHeader align="left">Pipeline</RunningHeader>
          <RunningHeader align="right">Duration</RunningHeader>
          <RunningHeader align="right">Rows In</RunningHeader>
          <RunningHeader align="right">Rows Out</RunningHeader>
          <RunningHeader align="right">Bytes In</RunningHeader>
        </div>
        {Array.from({ length: TOP_K_RUNNING }, (_, i) => {
          const task = tasks[i];
          return task ? (
            <RunningTaskRow key={task.task_id} task={task} now={now} />
          ) : (
            <div key={`empty-${i}`} className={`grid ${RUNNING_GRID_COLS} gap-0 items-center min-h-[36px]`} />
          );
        })}
        {totalRunning > tasks.length && (
          <div className={`${main.className} px-4 py-1.5 text-xs text-zinc-500 italic border-t border-zinc-800/50`}>
            Showing {tasks.length} of {totalRunning} running tasks (longest first)
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

function RunningTaskRow({ task, now }: { task: TaskInfo; now: number }) {
  // TODO: show cpu_us instead of wall-clock once within-task metric updates land
  const duration = formatDuration(Math.max(0, now - task.submit_sec)) + "\u2026";
  const pipeline = task.name
    ? task.name.includes("->") ? task.name.split("->") : [task.name]
    : [`Node ${task.origin_node_id}`];

  return (
    <div
      className={`grid ${RUNNING_GRID_COLS} gap-0 items-center min-h-[36px] hover:bg-zinc-800/40 transition-colors`}
    >
      <div className="px-3">
        <PipelineChips pipeline={pipeline} />
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-emerald-300 font-mono`}>
        {duration}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {task.rows_in.toLocaleString()}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {task.rows_out.toLocaleString()}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatBytes(task.bytes_in)}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Table of task-type rows.
// ---------------------------------------------------------------------------
const GRID_COLS =
  "grid-cols-[32px_minmax(220px,2fr)_minmax(180px,1.5fr)_90px_130px_110px_110px_110px_110px_100px]";

function TaskTypeTable({
  rows,
  onSelectOrigin,
}: {
  rows: TaskTypeRow[];
  onSelectOrigin: (nodeId: number) => void;
}) {
  return (
    <div className="min-w-[1250px]">
      {/* Header */}
      <div
        className={`bg-zinc-800 grid ${GRID_COLS} gap-0 items-center min-h-[48px] border-b border-zinc-700 sticky top-0 z-10`}
      >
        <HeaderCell />
        <HeaderCell align="left">Pipeline</HeaderCell>
        <HeaderCell align="left">Origin Node</HeaderCell>
        <HeaderCell align="right">Tasks</HeaderCell>
        <HeaderCell align="left">Status</HeaderCell>
        <HeaderCell align="right">Rows In</HeaderCell>
        <HeaderCell align="right">Rows Out</HeaderCell>
        <HeaderCell align="right">Bytes In</HeaderCell>
        <HeaderCell align="right">Bytes Out</HeaderCell>
        <HeaderCell align="right" last>
          CPU
        </HeaderCell>
      </div>

      {/* Body */}
      <div className="divide-y divide-zinc-800">
        {rows.map((row) => (
          <TaskTypeGroupRow key={row.key} row={row} onSelectOrigin={onSelectOrigin} />
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
  onSelectOrigin,
}: {
  row: TaskTypeRow;
  onSelectOrigin: (nodeId: number) => void;
}) {
  const [expanded, setExpanded] = useState(false);

  return (
    <>
      <div
        className={`grid ${GRID_COLS} gap-0 items-center min-h-[48px] cursor-pointer hover:bg-zinc-800/50 transition-colors`}
        onClick={() => setExpanded((e) => !e)}
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
          <button
            onClick={(e) => {
              e.stopPropagation();
              onSelectOrigin(row.origin_node_id);
            }}
            className={`${main.className} text-sm text-sky-400 hover:text-sky-300 hover:underline font-mono text-left truncate`}
            title={`Jump to ${row.origin_node_name} in the physical plan`}
          >
            {row.origin_node_name}{" "}
            <span className="text-zinc-500">#{row.origin_node_id}</span>
          </button>
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
          <Mono>{row.total_rows_in.toLocaleString()}</Mono>
        </Cell>
        <Cell align="right">
          <Mono>{row.total_rows_out.toLocaleString()}</Mono>
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
// Removed the Context column since the backend does not yet surface per-task
// context (filename, partition id, etc.). See TODO below.
const SUB_GRID_COLS =
  "grid-cols-[32px_80px_minmax(140px,1.5fr)_110px_100px_110px_110px_110px_110px_100px]";

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
        <SubHeader align="right">Duration</SubHeader>
        <SubHeader align="right">Rows In</SubHeader>
        <SubHeader align="right">Rows Out</SubHeader>
        <SubHeader align="right">Bytes In</SubHeader>
        <SubHeader align="right">Bytes Out</SubHeader>
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
          (longest by duration, plus active and failed)
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
        {duration}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {task.rows_in.toLocaleString()}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {task.rows_out.toLocaleString()}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatBytes(task.bytes_in)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatBytes(task.bytes_out)}
      </div>
      <div className={`${main.className} px-3 text-xs text-right text-zinc-300 font-mono`}>
        {formatDuration(cpuSec)}
      </div>
    </div>
  );
}
