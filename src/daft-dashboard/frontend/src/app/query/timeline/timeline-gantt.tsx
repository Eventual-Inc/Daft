"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { main } from "@/lib/utils";
import { OperatorInfo, OperatorStatus, Stat } from "../types";
import { formatDuration, formatStatValue, getStatusText } from "../stats-utils";

type ProcessStatsSample = [number, Record<string, Stat>];

type GanttProps = {
  operators: Record<number, OperatorInfo>;
  query_start_sec: number;
  query_end_sec: number | null;
  process_stats?: ProcessStatsSample[];
};

const PROCESS_CPU_KEY = "process.cpu.percent";
const PROCESS_RSS_KEY = "process.memory.rss";

const CPU_LINE_COLOR = "rgb(56, 189, 248)";
const MEM_LINE_COLOR = "rgb(168, 85, 247)";
const OVERLAY_TRACK_HEIGHT = 90;
const OVERLAY_TOP_PAD = 10;
const OVERLAY_BOTTOM_PAD = 6;

function formatBytes(b: number): string {
  if (b >= 1024 * 1024 * 1024) return `${(b / (1024 ** 3)).toFixed(1)} GiB`;
  if (b >= 1024 * 1024) return `${(b / (1024 ** 2)).toFixed(1)} MiB`;
  if (b >= 1024) return `${(b / 1024).toFixed(1)} KiB`;
  return `${b} B`;
}

function getStatValue(stat: Stat | undefined): number | null {
  if (!stat) return null;
  if (stat.type === "Count" || stat.type === "Bytes" || stat.type === "Percent" || stat.type === "Float") {
    return stat.value;
  }
  return null;
}

const LABEL_WIDTH = 220;
const ROW_HEIGHT = 26;
const ROW_GAP = 4;
const AXIS_HEIGHT = 28;
const MIN_PX_PER_SEC = 1; // effectively "fit to viewport" by default
const MIN_BAR_WIDTH = 2;

const STATUS_FILL: Record<OperatorStatus, string> = {
  Finished: "rgb(20, 83, 45)",
  Executing: "rgb(146, 64, 14)",
  Failed: "rgb(127, 29, 29)",
  Pending: "rgb(39, 39, 42)",
};

const STATUS_STROKE: Record<OperatorStatus, string> = {
  Finished: "rgb(34, 122, 64)",
  Executing: "rgb(217, 119, 6)",
  Failed: "rgb(185, 28, 28)",
  Pending: "rgb(82, 82, 91)",
};

const NICE_INTERVALS = [
  0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 1800, 3600,
];

function pickTickInterval(totalSec: number, targetTicks = 8): number {
  const raw = totalSec / targetTicks;
  for (const candidate of NICE_INTERVALS) {
    if (candidate >= raw) return candidate;
  }
  return NICE_INTERVALS[NICE_INTERVALS.length - 1];
}

function formatAxisLabel(sec: number): string {
  if (sec < 1) return `${Math.round(sec * 1000)}ms`;
  if (sec < 60) return `${sec.toFixed(sec < 10 ? 1 : 0)}s`;
  const mins = Math.floor(sec / 60);
  const rem = Math.round(sec % 60);
  return rem === 0 ? `${mins}m` : `${mins}m${rem}s`;
}

type TimelineRow = {
  id: number;
  operator: OperatorInfo;
  offsetSec: number;
  durationSec: number;
};

export default function TimelineGantt({
  operators,
  query_start_sec,
  query_end_sec,
  process_stats,
}: GanttProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(1200);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [now, setNow] = useState(() => Date.now() / 1000);

  const isLive = query_end_sec == null;

  useEffect(() => {
    if (!isLive) return;
    const id = setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => clearInterval(id);
  }, [isLive]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(entries => {
      for (const entry of entries) {
        setContainerWidth(entry.contentRect.width);
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const { rows, totalSec } = useMemo(() => {
    const effectiveEnd = query_end_sec ?? now;
    const rawRows: TimelineRow[] = Object.entries(operators)
      .map(([idStr, op]) => {
        const id = Number(idStr);
        if (op.start_sec == null) return null;
        const end = op.end_sec ?? (op.status === "Executing" ? effectiveEnd : op.start_sec);
        return {
          id,
          operator: op,
          offsetSec: Math.max(0, op.start_sec - query_start_sec),
          durationSec: Math.max(0, end - op.start_sec),
        };
      })
      .filter((r): r is TimelineRow => r !== null)
      .sort((a, b) => a.offsetSec - b.offsetSec || a.id - b.id);

    const lastEndSec = rawRows.reduce(
      (acc, r) => Math.max(acc, r.offsetSec + r.durationSec),
      0,
    );
    const queryDurationSec = Math.max(0, effectiveEnd - query_start_sec);
    return {
      rows: rawRows,
      totalSec: Math.max(lastEndSec, queryDurationSec, 0.5),
    };
  }, [operators, query_start_sec, query_end_sec, now]);

  const availableWidth = Math.max(400, containerWidth - LABEL_WIDTH - 24);
  const pxPerSec = Math.max(MIN_PX_PER_SEC, availableWidth / totalSec);
  const chartWidth = totalSec * pxPerSec;
  const chartHeight = rows.length * (ROW_HEIGHT + ROW_GAP);

  const processSeries = useMemo(() => {
    if (!process_stats || process_stats.length === 0) {
      return { cpu: [] as [number, number][], mem: [] as [number, number][], maxMemBytes: 0 };
    }
    const cpu: [number, number][] = [];
    const mem: [number, number][] = [];
    let maxMemBytes = 0;
    for (const [ts, metrics] of process_stats) {
      const offsetSec = ts - query_start_sec;
      if (offsetSec < -0.5 || offsetSec > totalSec + 0.5) continue;
      const cpuVal = getStatValue(metrics[PROCESS_CPU_KEY]);
      const memVal = getStatValue(metrics[PROCESS_RSS_KEY]);
      if (cpuVal != null) cpu.push([offsetSec, cpuVal]);
      if (memVal != null) {
        mem.push([offsetSec, memVal]);
        if (memVal > maxMemBytes) maxMemBytes = memVal;
      }
    }
    cpu.sort((a, b) => a[0] - b[0]);
    mem.sort((a, b) => a[0] - b[0]);
    return { cpu, mem, maxMemBytes };
  }, [process_stats, query_start_sec, totalSec]);

  const peakCpu = processSeries.cpu.reduce((m, [, v]) => Math.max(m, v), 0);
  const cpuAxisMax = Math.max(100, Math.ceil(peakCpu / 100) * 100);
  const hasOverlay = processSeries.cpu.length > 0 || processSeries.mem.length > 0;

  const trackHeight = hasOverlay ? OVERLAY_TRACK_HEIGHT : 0;
  const overlayTop = AXIS_HEIGHT + OVERLAY_TOP_PAD;
  const overlayBottom = AXIS_HEIGHT + trackHeight - OVERLAY_BOTTOM_PAD;
  const overlayInnerHeight = Math.max(1, overlayBottom - overlayTop);
  const rowsTop = AXIS_HEIGHT + trackHeight;
  const svgHeight = rowsTop + chartHeight + 8;

  const tickInterval = pickTickInterval(totalSec);
  const ticks: number[] = [];
  for (let t = 0; t <= totalSec + tickInterval * 0.5; t += tickInterval) {
    ticks.push(t);
  }

  const selectedRow = rows.find(r => r.id === selectedId);

  const toCpuPath = (pts: [number, number][]): string => {
    if (pts.length === 0) return "";
    return pts
      .map(([t, v], i) => {
        const x = LABEL_WIDTH + t * pxPerSec;
        const clamped = Math.max(0, Math.min(cpuAxisMax, v));
        const y = overlayBottom - (clamped / cpuAxisMax) * overlayInnerHeight;
        return `${i === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
      })
      .join(" ");
  };

  const toMemPath = (pts: [number, number][], maxBytes: number): string => {
    if (pts.length === 0 || maxBytes <= 0) return "";
    return pts
      .map(([t, v], i) => {
        const x = LABEL_WIDTH + t * pxPerSec;
        const frac = Math.max(0, Math.min(1, v / maxBytes));
        const y = overlayBottom - frac * overlayInnerHeight;
        return `${i === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
      })
      .join(" ");
  };

  const cpuPath = toCpuPath(processSeries.cpu);
  const memPath = toMemPath(processSeries.mem, processSeries.maxMemBytes);

  if (rows.length === 0) {
    return (
      <div
        ref={containerRef}
        className="bg-zinc-900 h-full flex items-center justify-center"
      >
        <p className={`${main.className} text-zinc-400`}>
          No operator activity yet
        </p>
      </div>
    );
  }

  return (
    <div ref={containerRef} className="bg-zinc-900 h-full flex">
      <div className="flex-1 overflow-auto relative">
        {hasOverlay && (
          <div
            className={`${main.className} absolute top-2 right-4 z-10 flex gap-3 text-[10px] font-mono bg-zinc-900/80 border border-zinc-800 rounded px-2 py-1`}
          >
            {cpuPath && (
              <span className="flex items-center gap-1.5 text-zinc-300">
                <span
                  className="inline-block w-3 h-0.5"
                  style={{ backgroundColor: CPU_LINE_COLOR }}
                />
                CPU (peak {peakCpu.toFixed(0)}%)
              </span>
            )}
            {memPath && (
              <span className="flex items-center gap-1.5 text-zinc-300">
                <span
                  className="inline-block w-3 h-0.5"
                  style={{ backgroundColor: MEM_LINE_COLOR }}
                />
                RSS (peak {formatBytes(processSeries.maxMemBytes)})
              </span>
            )}
          </div>
        )}
        <svg
          width={LABEL_WIDTH + chartWidth + 24}
          height={svgHeight}
          className="block"
        >
          {/* Axis ticks + gridlines (span overlay track + rows) */}
          <g transform={`translate(${LABEL_WIDTH}, 0)`}>
            {ticks.map((t, i) => {
              const x = t * pxPerSec;
              return (
                <g key={i}>
                  <line
                    x1={x}
                    y1={AXIS_HEIGHT - 6}
                    x2={x}
                    y2={rowsTop + chartHeight}
                    stroke="rgb(39, 39, 42)"
                    strokeWidth={1}
                  />
                  <text
                    x={x + 3}
                    y={AXIS_HEIGHT - 10}
                    fill="rgb(161, 161, 170)"
                    fontSize={10}
                    fontFamily="ui-monospace, monospace"
                  >
                    {formatAxisLabel(t)}
                  </text>
                </g>
              );
            })}
          </g>

          {/* Process metrics track (dedicated strip between axis and rows) */}
          {hasOverlay && (
            <g>
              {/* Track background */}
              <rect
                x={0}
                y={AXIS_HEIGHT}
                width={LABEL_WIDTH + chartWidth}
                height={trackHeight}
                fill="rgb(17, 17, 19)"
              />
              {/* Track bottom border */}
              <line
                x1={0}
                y1={AXIS_HEIGHT + trackHeight}
                x2={LABEL_WIDTH + chartWidth}
                y2={AXIS_HEIGHT + trackHeight}
                stroke="rgb(39, 39, 42)"
                strokeWidth={1}
              />
              {/* 50% gridline */}
              <line
                x1={LABEL_WIDTH}
                y1={overlayTop + overlayInnerHeight / 2}
                x2={LABEL_WIDTH + chartWidth}
                y2={overlayTop + overlayInnerHeight / 2}
                stroke="rgb(39, 39, 42)"
                strokeWidth={1}
                strokeDasharray="2 3"
              />
              {/* Y-axis labels on the left */}
              <text
                x={LABEL_WIDTH - 8}
                y={overlayTop + 4}
                fill="rgb(113, 113, 122)"
                fontSize={9}
                fontFamily="ui-monospace, monospace"
                textAnchor="end"
              >
                {cpuAxisMax}%
              </text>
              <text
                x={LABEL_WIDTH - 8}
                y={overlayBottom + 3}
                fill="rgb(113, 113, 122)"
                fontSize={9}
                fontFamily="ui-monospace, monospace"
                textAnchor="end"
              >
                0
              </text>
              {/* Track label */}
              <text
                x={12}
                y={AXIS_HEIGHT + 14}
                fill="rgb(161, 161, 170)"
                fontSize={10}
                fontFamily="ui-monospace, monospace"
                className="select-none"
              >
                Process
              </text>
              {/* Curves — full opacity now that they have their own space */}
              {memPath && (
                <path
                  d={memPath}
                  fill="none"
                  stroke={MEM_LINE_COLOR}
                  strokeWidth={1.75}
                  strokeLinejoin="round"
                  strokeLinecap="round"
                />
              )}
              {cpuPath && (
                <path
                  d={cpuPath}
                  fill="none"
                  stroke={CPU_LINE_COLOR}
                  strokeWidth={1.75}
                  strokeLinejoin="round"
                  strokeLinecap="round"
                />
              )}
            </g>
          )}

          {/* Operator rows */}
          {rows.map((row, i) => {
            const y = rowsTop + i * (ROW_HEIGHT + ROW_GAP);
            const barX = LABEL_WIDTH + row.offsetSec * pxPerSec;
            const barW = Math.max(MIN_BAR_WIDTH, row.durationSec * pxPerSec);
            const fill = STATUS_FILL[row.operator.status];
            const stroke = STATUS_STROKE[row.operator.status];
            const isSelected = row.id === selectedId;
            const label = row.operator.node_info.name;
            const durationText = formatDuration(row.durationSec);

            return (
              <g
                key={row.id}
                className="cursor-pointer"
                onClick={() => setSelectedId(row.id === selectedId ? null : row.id)}
              >
                {/* Row hover background */}
                <rect
                  x={0}
                  y={y - ROW_GAP / 2}
                  width={LABEL_WIDTH + chartWidth}
                  height={ROW_HEIGHT + ROW_GAP}
                  fill={isSelected ? "rgba(255, 255, 255, 0.04)" : "transparent"}
                />

                {/* Label */}
                <text
                  x={12}
                  y={y + ROW_HEIGHT / 2 + 4}
                  fill="rgb(228, 228, 231)"
                  fontSize={12}
                  fontFamily="ui-monospace, monospace"
                  className="select-none"
                >
                  {label.length > 26 ? `${label.slice(0, 25)}…` : label}
                </text>

                {/* Bar */}
                <rect
                  x={barX}
                  y={y}
                  width={barW}
                  height={ROW_HEIGHT}
                  rx={3}
                  ry={3}
                  fill={fill}
                  stroke={stroke}
                  strokeWidth={isSelected ? 2 : 1}
                />

                {/* Duration label — inside the bar if it fits, else to the right */}
                {barW > 48 ? (
                  <text
                    x={barX + 6}
                    y={y + ROW_HEIGHT / 2 + 4}
                    fill="rgb(244, 244, 245)"
                    fontSize={11}
                    fontFamily="ui-monospace, monospace"
                    className="select-none"
                  >
                    {durationText}
                  </text>
                ) : (
                  <text
                    x={barX + barW + 6}
                    y={y + ROW_HEIGHT / 2 + 4}
                    fill="rgb(161, 161, 170)"
                    fontSize={11}
                    fontFamily="ui-monospace, monospace"
                    className="select-none"
                  >
                    {durationText}
                  </text>
                )}

                <title>
                  {label}
                  {"\n"}
                  {getStatusText(row.operator.status)} · {durationText}
                  {"\n"}
                  start +{formatDuration(row.offsetSec)}
                </title>
              </g>
            );
          })}
        </svg>
      </div>

      {/* Details side panel */}
      {selectedRow && (
        <div className="w-80 border-l border-zinc-800 p-4 overflow-auto flex-shrink-0">
          <div className="flex items-start justify-between gap-2 mb-3">
            <div>
              <h3 className={`${main.className} text-sm font-bold text-zinc-100 break-all`}>
                {selectedRow.operator.node_info.name}
              </h3>
              <p className={`${main.className} text-xs text-zinc-500 mt-0.5`}>
                {selectedRow.operator.node_info.node_category}
              </p>
            </div>
            <button
              onClick={() => setSelectedId(null)}
              className="text-zinc-500 hover:text-zinc-200 text-lg leading-none"
              aria-label="Close details"
            >
              ×
            </button>
          </div>
          <dl className="space-y-2 text-xs">
            <Field label="Status" value={getStatusText(selectedRow.operator.status)} />
            <Field label="Duration" value={formatDuration(selectedRow.durationSec)} />
            <Field label="Started +" value={formatDuration(selectedRow.offsetSec)} />
            {Object.entries(selectedRow.operator.stats).length > 0 && (
              <>
                <div className="pt-2 border-t border-zinc-800">
                  <p
                    className={`${main.className} text-[10px] uppercase tracking-wider text-zinc-500 mb-1.5`}
                  >
                    Stats
                  </p>
                </div>
                {Object.entries(selectedRow.operator.stats)
                  .sort(([a], [b]) => a.localeCompare(b))
                  .map(([key, stat]) => (
                    <Field key={key} label={key} value={formatStatValue(stat)} />
                  ))}
              </>
            )}
          </dl>
        </div>
      )}
    </div>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-2">
      <dt className={`${main.className} text-zinc-500`}>{label}</dt>
      <dd className={`${main.className} text-zinc-200 font-mono text-right break-all`}>
        {value}
      </dd>
    </div>
  );
}
