"use client";

import { useMemo, useState } from "react";
import { ChevronDown, ChevronRight, X } from "lucide-react";
import { main } from "@/lib/utils";
import { ExecutingState, OperatorStatus } from "./types";
import {
  formatBytes,
  formatDuration,
  getStatusIcon,
  getStatusColor,
  getStatusText,
} from "./stats-utils";
import {
  generateMockTasks,
  groupTasks,
  SwordfishTask,
  TaskTypeRow,
} from "./mock-tasks";

/**
 * Tasks tab — a table of swordfish task "types" (fused local-plan pipelines
 * grouped by origin distributed-plan node). Rows are expandable to the
 * individual task level.
 *
 * Mocked today: tasks are synthesized from the live operator list. Replace
 * `generateMockTasks` with a real backend feed when available.
 */
export default function TasksTab({
  exec_state,
  queryId,
  originFilter,
  onClearFilter,
  onSelectOrigin,
}: {
  exec_state: ExecutingState;
  queryId: string;
  /** If set, only show rows whose origin_node_id matches. */
  originFilter: number | null;
  /** Called when the user clears the origin filter. */
  onClearFilter: () => void;
  /** Called when the user clicks an origin node name — navigates to Execution tab + highlight. */
  onSelectOrigin: (nodeId: number) => void;
}) {
  const { operators, exec_start_sec } = exec_state.exec_info;

  const allTasks = useMemo(
    () => generateMockTasks(operators, exec_start_sec, queryId),
    [operators, exec_start_sec, queryId],
  );

  const allRows = useMemo(() => groupTasks(allTasks, operators), [allTasks, operators]);

  const rows = useMemo(
    () =>
      originFilter == null
        ? allRows
        : allRows.filter((r) => r.origin_node_id === originFilter),
    [allRows, originFilter],
  );

  const filteredOriginName =
    originFilter != null
      ? (operators[originFilter]?.node_info.name ?? `Node ${originFilter}`)
      : null;

  return (
    <div className="bg-zinc-900 h-full flex flex-col">
      <div className="flex items-center justify-between gap-3 px-4 pt-3 pb-2 border-b border-zinc-800">
        <div className={`${main.className} text-xs text-zinc-400 flex items-center gap-2`}>
          <span className="text-zinc-500">Mock data —</span>
          <span>
            {rows.length} task type{rows.length === 1 ? "" : "s"} across{" "}
            {rows.reduce((a, r) => a + r.task_count, 0)} task
            {rows.reduce((a, r) => a + r.task_count, 0) === 1 ? "" : "s"}
          </span>
          {originFilter != null && (
            <span className="flex items-center gap-1 px-2 py-0.5 rounded-md bg-fuchsia-950/50 border border-fuchsia-800 text-fuchsia-200">
              origin: {filteredOriginName} (#{originFilter})
              <button
                onClick={onClearFilter}
                className="hover:text-white"
                aria-label="Clear origin filter"
              >
                <X size={12} />
              </button>
            </span>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-auto">
        {rows.length === 0 ? (
          <div className="p-8 text-center">
            <p className={`${main.className} text-zinc-400`}>
              No tasks for this filter.
            </p>
          </div>
        ) : (
          <TaskTypeTable rows={rows} onSelectOrigin={onSelectOrigin} />
        )}
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

      {expanded && <ExpandedTaskList tasks={row.tasks} />}
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
const SUB_GRID_COLS =
  "grid-cols-[32px_80px_120px_110px_100px_110px_110px_110px_110px_100px_1fr]";

function ExpandedTaskList({ tasks }: { tasks: SwordfishTask[] }) {
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
        <SubHeader align="left">Context</SubHeader>
      </div>
      {[...tasks]
        .sort((a, b) => a.task_id - b.task_id)
        .map((t) => (
          <TaskSubRow key={t.task_id} task={t} />
        ))}
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

function TaskSubRow({ task }: { task: SwordfishTask }) {
  const duration =
    task.end_sec != null
      ? formatDuration(task.end_sec - task.start_sec)
      : task.status === "Executing"
        ? formatDuration(Date.now() / 1000 - task.start_sec) + "…"
        : "—";

  const contextParts = Object.entries(task.context);
  const contextStr = contextParts.map(([k, v]) => `${k}=${v}`).join("  ");

  return (
    <div
      className={`grid ${SUB_GRID_COLS} gap-0 items-center min-h-[36px] hover:bg-zinc-800/40 transition-colors`}
    >
      <div className="flex items-center justify-center">
        {getStatusIcon(task.status)}
      </div>
      <div className={`${main.className} px-3 text-xs text-zinc-400 font-mono`}>
        {task.task_id}
      </div>
      <div
        className={`${main.className} px-3 text-xs text-zinc-300 font-mono truncate`}
        title={`${task.worker_id} (${task.ip_address})`}
      >
        {task.worker_id}
      </div>
      <div className={`${main.className} px-3 text-xs font-mono ${getStatusColor(task.status)}`}>
        {getStatusText(task.status)}
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
        {formatDuration(task.cpu_sec)}
      </div>
      <div
        className={`${main.className} px-3 text-xs text-zinc-400 font-mono truncate`}
        title={contextStr}
      >
        {contextStr || "—"}
      </div>
    </div>
  );
}
