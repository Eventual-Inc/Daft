"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { main } from "@/lib/utils";
import { OperatorInfo, Stat } from "../types";
import { formatDuration, formatStatValue, getStatusText } from "../stats-utils";

type ProcessStatsSample = [number, Record<string, Stat>];

type SparklinesProps = {
  operators: Record<number, OperatorInfo>;
  query_start_sec: number;
  query_end_sec: number | null;
  process_stats?: ProcessStatsSample[];
};

const PROCESS_CPU_KEY = "process.cpu.percent";
const PROCESS_RSS_KEY = "process.memory.rss";
const ROWS_IN_KEY = "rows.in";
const ROWS_OUT_KEY = "rows.out";
const BYTES_IN_KEY = "bytes.in";
const BYTES_OUT_KEY = "bytes.out";
const DURATION_KEY = "duration";

type BufferMetric = "rows" | "bytes" | "cpu";

const CPU_LINE_COLOR = "rgb(56, 189, 248)";
const MEM_LINE_COLOR = "rgb(168, 85, 247)";
const BAR_FILL_COLOR = "rgba(52, 211, 153, 0.75)";
const OVERLAY_TRACK_HEIGHT = 90;
const OVERLAY_TOP_PAD = 10;
const OVERLAY_BOTTOM_PAD = 6;

const LABEL_WIDTH = 220;
const SUMMARY_WIDTH = 160;
const ROW_HEIGHT = 24;
const ROW_GAP = 4;
const ROW_PAD_Y = 3;
const AXIS_HEIGHT = 28;
const MIN_PX_PER_SEC = 1; // effectively "fit to viewport" by default

function formatBytes(b: number): string {
  if (b >= 1024 * 1024 * 1024) return `${(b / 1024 ** 3).toFixed(1)} GiB`;
  if (b >= 1024 * 1024) return `${(b / 1024 ** 2).toFixed(1)} MiB`;
  if (b >= 1024) return `${(b / 1024).toFixed(1)} KiB`;
  return `${b} B`;
}

function formatCount(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
  return n.toLocaleString();
}

function getStatValue(stat: Stat | undefined): number | null {
  if (!stat) return null;
  if (stat.type === "Count" || stat.type === "Bytes" || stat.type === "Percent" || stat.type === "Float") {
    return stat.value;
  }
  return null;
}

function getDurationSec(stat: Stat | undefined): number | null {
  if (!stat) return null;
  if (stat.type === "Duration") {
    return stat.value.secs + stat.value.nanos / 1e9;
  }
  return null;
}

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

type SparkRow = {
  id: number;
  operator: OperatorInfo;
  points: [number, number][]; // (offsetSec, bufferDepth — rows OR bytes)
  peakBuffer: number;
  avgBuffer: number;
  finalRows: number;
  lifetimeStartOffsetSec: number; // >= 0
  lifetimeEndOffsetSec: number; // >= lifetimeStartOffsetSec
};

function buildSparkRow(
  id: number,
  operator: OperatorInfo,
  query_start_sec: number,
  effectiveEndOffsetSec: number,
  metric: BufferMetric,
): SparkRow | null {
  if (operator.start_sec == null) return null;

  const history = operator.stats_history ?? [];
  const lifetimeStartOffsetSec = Math.max(0, operator.start_sec - query_start_sec);
  const lifetimeEndOffsetSec = Math.max(
    lifetimeStartOffsetSec,
    operator.end_sec != null
      ? operator.end_sec - query_start_sec
      : effectiveEndOffsetSec,
  );

  const rawPoints: [number, number][] = [];

  if (metric === "cpu") {
    // Cumulative CPU time (monotonic). Universally non-zero for any op
    // doing real work, including sources and pass-through transforms.
    for (const [ts, stats] of history) {
      const cpuSec = getDurationSec(stats[DURATION_KEY]);
      if (cpuSec == null) continue;
      const offsetSec = ts - query_start_sec;
      if (!isFinite(offsetSec)) continue;
      rawPoints.push([offsetSec, Math.max(0, cpuSec)]);
    }
  } else {
    const inKey = metric === "rows" ? ROWS_IN_KEY : BYTES_IN_KEY;
    const outKey = metric === "rows" ? ROWS_OUT_KEY : BYTES_OUT_KEY;
    // Buffer depth = rows/bytes currently inside the operator = in - out.
    // Clamped to zero so reductive ops and clock jitter don't drag negative.
    for (const [ts, stats] of history) {
      const inVal = getStatValue(stats[inKey]);
      const outVal = getStatValue(stats[outKey]);
      if (inVal == null && outVal == null) continue;
      const offsetSec = ts - query_start_sec;
      if (!isFinite(offsetSec)) continue;
      const buf = Math.max(0, (inVal ?? 0) - (outVal ?? 0));
      rawPoints.push([offsetSec, buf]);
    }
  }
  rawPoints.sort((a, b) => a[0] - b[0]);

  // Pad the start: buffer is empty before the first morsel arrives.
  const points: [number, number][] = [];
  if (rawPoints.length === 0 || rawPoints[0][0] > lifetimeStartOffsetSec) {
    points.push([lifetimeStartOffsetSec, 0]);
  }
  for (const p of rawPoints) {
    if (p[0] < lifetimeStartOffsetSec) {
      points.push([lifetimeStartOffsetSec, p[1]]);
    } else {
      points.push(p);
    }
  }

  // Pad the end: cumulative metrics (CPU time) keep their last value.
  // Buffer metrics drop to 0 when the op has finished (buffer drained),
  // or hold the last value while still running.
  const last = points[points.length - 1];
  if (last[0] < lifetimeEndOffsetSec) {
    const cumulative = metric === "cpu";
    const tail = cumulative
      ? last[1]
      : operator.end_sec != null
        ? 0
        : last[1];
    points.push([lifetimeEndOffsetSec, tail]);
  }

  let peakBuffer = 0;
  let sumBuffer = 0;
  for (const [, v] of points) {
    if (v > peakBuffer) peakBuffer = v;
    sumBuffer += v;
  }
  const avgBuffer = points.length > 0 ? sumBuffer / points.length : 0;

  const finalRows =
    history.length > 0
      ? getStatValue(history[history.length - 1][1][ROWS_OUT_KEY]) ?? 0
      : 0;

  return {
    id,
    operator,
    points,
    peakBuffer,
    avgBuffer,
    finalRows,
    lifetimeStartOffsetSec,
    lifetimeEndOffsetSec,
  };
}

export default function SparklinesView({
  operators,
  query_start_sec,
  query_end_sec,
  process_stats,
}: SparklinesProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(1200);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [now, setNow] = useState(() => Date.now() / 1000);
  const [metric, setMetric] = useState<BufferMetric>("rows");

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
    const effectiveEndOffsetSec = Math.max(0, effectiveEnd - query_start_sec);
    const rawRows: SparkRow[] = Object.entries(operators)
      .map(([idStr, op]) =>
        buildSparkRow(
          Number(idStr),
          op,
          query_start_sec,
          effectiveEndOffsetSec,
          metric,
        ),
      )
      .filter((row): row is SparkRow => row !== null)
      .sort(
        (a, b) =>
          a.lifetimeStartOffsetSec - b.lifetimeStartOffsetSec || a.id - b.id,
      );

    return {
      rows: rawRows,
      totalSec: Math.max(effectiveEndOffsetSec, 0.5),
    };
  }, [operators, query_start_sec, query_end_sec, now, metric]);

  const availableWidth = Math.max(
    400,
    containerWidth - LABEL_WIDTH - SUMMARY_WIDTH - 24,
  );
  const pxPerSec = Math.max(MIN_PX_PER_SEC, availableWidth / totalSec);
  const chartWidth = totalSec * pxPerSec;
  const chartHeight = rows.length * (ROW_HEIGHT + ROW_GAP);

  // Global Y scale — same peak across all rows so magnitudes are comparable.
  const globalPeakBuffer = rows.reduce(
    (m, r) => Math.max(m, r.peakBuffer),
    0,
  );

  // Top ops for the bottom report, ranked by sustained buffer (avg with
  // peak as tiebreaker). Uses avg so one transient spike doesn't dominate.
  const topOps = [...rows]
    .filter(r => r.peakBuffer > 0)
    .sort((a, b) => {
      if (b.avgBuffer !== a.avgBuffer) return b.avgBuffer - a.avgBuffer;
      return b.peakBuffer - a.peakBuffer;
    })
    .slice(0, 5);

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

  const toProcessPath = (
    pts: [number, number][],
    scaleTo: (v: number) => number,
  ): string => {
    if (pts.length === 0) return "";
    return pts
      .map(([t, v], i) => {
        const x = LABEL_WIDTH + t * pxPerSec;
        const y = overlayBottom - scaleTo(v) * overlayInnerHeight;
        return `${i === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
      })
      .join(" ");
  };

  const cpuPath = toProcessPath(processSeries.cpu, v =>
    Math.max(0, Math.min(1, v / cpuAxisMax)),
  );
  const memPath = toProcessPath(processSeries.mem, v =>
    processSeries.maxMemBytes > 0
      ? Math.max(0, Math.min(1, v / processSeries.maxMemBytes))
      : 0,
  );

  const selectedRow = rows.find(r => r.id === selectedId);

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

  const buildBars = (
    points: [number, number][],
    peak: number,
    innerHeight: number,
    baseY: number,
    lifetimeEndSec: number,
  ): { x: number; y: number; width: number; height: number }[] => {
    if (points.length === 0 || peak <= 0) return [];
    const bottomY = baseY + innerHeight;
    // Log10-scaled so rows at 100 KiB and 1 GiB are both visually meaningful.
    const logPeak = Math.log10(1 + peak);
    const bars: { x: number; y: number; width: number; height: number }[] = [];
    for (let i = 0; i < points.length; i++) {
      const [t, v] = points[i];
      const nextT = i < points.length - 1 ? points[i + 1][0] : lifetimeEndSec;
      const span = nextT - t;
      if (span <= 0) continue;
      const logV = Math.log10(1 + Math.max(0, v));
      const frac = logPeak > 0 ? logV / logPeak : 0;
      const barHeight = frac * innerHeight;
      if (barHeight < 0.5) continue;
      const x = LABEL_WIDTH + t * pxPerSec;
      const width = Math.max(1, span * pxPerSec - 0.5);
      bars.push({ x, y: bottomY - barHeight, width, height: barHeight });
    }
    return bars;
  };

  const handleSelect = (id: number) =>
    setSelectedId(prev => (prev === id ? null : id));

  const formatBufferValue = (v: number): string => {
    if (metric === "bytes") return formatBytes(v);
    if (metric === "cpu") return formatDuration(v);
    return formatCount(v);
  };
  const bufferLabel =
    metric === "bytes" ? "bytes" : metric === "cpu" ? "cpu time" : "rows";
  const metricLegendLabel =
    metric === "cpu" ? "CPU time" : "Buffer";

  const mainSvgWidth = LABEL_WIDTH + chartWidth + 24;

  return (
    <div ref={containerRef} className="bg-zinc-900 h-full flex">
      <div className="flex-1 min-w-0 flex flex-col">
        <div
          className={`${main.className} flex-shrink-0 flex items-center justify-end gap-4 px-4 py-1.5 border-b border-zinc-800 text-[10px] font-mono`}
        >
          {hasOverlay && cpuPath && (
            <span className="flex items-center gap-1.5 text-zinc-300">
              <span
                className="inline-block w-3 h-0.5"
                style={{ backgroundColor: CPU_LINE_COLOR }}
              />
              CPU (peak {peakCpu.toFixed(0)}%)
            </span>
          )}
          {hasOverlay && memPath && (
            <span className="flex items-center gap-1.5 text-zinc-300">
              <span
                className="inline-block w-3 h-0.5"
                style={{ backgroundColor: MEM_LINE_COLOR }}
              />
              RSS (peak {formatBytes(processSeries.maxMemBytes)})
            </span>
          )}
          <span className="flex items-center gap-1.5 text-zinc-300">
            <span
              className="inline-block w-3 h-2"
              style={{ backgroundColor: BAR_FILL_COLOR }}
            />
            {metricLegendLabel} (log scale, global max{" "}
            {formatBufferValue(globalPeakBuffer)})
          </span>
          <div className="flex items-center rounded-md border border-zinc-700 overflow-hidden">
            <button
              onClick={() => setMetric("rows")}
              className={`px-2 py-0.5 transition-colors ${
                metric === "rows"
                  ? "bg-zinc-700 text-zinc-100"
                  : "text-zinc-400 hover:text-zinc-200"
              }`}
            >
              rows
            </button>
            <button
              onClick={() => setMetric("bytes")}
              className={`px-2 py-0.5 transition-colors border-l border-zinc-700 ${
                metric === "bytes"
                  ? "bg-zinc-700 text-zinc-100"
                  : "text-zinc-400 hover:text-zinc-200"
              }`}
            >
              bytes
            </button>
            <button
              onClick={() => setMetric("cpu")}
              className={`px-2 py-0.5 transition-colors border-l border-zinc-700 ${
                metric === "cpu"
                  ? "bg-zinc-700 text-zinc-100"
                  : "text-zinc-400 hover:text-zinc-200"
              }`}
            >
              cpu
            </button>
          </div>
        </div>
        {/* Vertical scroll container; main + summary scroll together vertically */}
        <div className="flex-1 min-h-0 overflow-y-auto">
          <div className="flex" style={{ minHeight: "100%" }}>
            {/* Main: horizontally scrollable timeline. */}
            <div className="flex-1 min-w-0 overflow-x-auto">
            <svg
              width={mainSvgWidth}
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

          {/* Process metrics track (dedicated strip) */}
          {hasOverlay && (
            <g>
              <rect
                x={0}
                y={AXIS_HEIGHT}
                width={LABEL_WIDTH + chartWidth}
                height={trackHeight}
                fill="rgb(17, 17, 19)"
              />
              <line
                x1={0}
                y1={AXIS_HEIGHT + trackHeight}
                x2={LABEL_WIDTH + chartWidth}
                y2={AXIS_HEIGHT + trackHeight}
                stroke="rgb(39, 39, 42)"
                strokeWidth={1}
              />
              <line
                x1={LABEL_WIDTH}
                y1={overlayTop + overlayInnerHeight / 2}
                x2={LABEL_WIDTH + chartWidth}
                y2={overlayTop + overlayInnerHeight / 2}
                stroke="rgb(39, 39, 42)"
                strokeWidth={1}
                strokeDasharray="2 3"
              />
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

          {/* Operator sparkline rows */}
          {rows.map((row, i) => {
            const rowY = rowsTop + i * (ROW_HEIGHT + ROW_GAP);
            const innerY = rowY + ROW_PAD_Y;
            const innerHeight = ROW_HEIGHT - ROW_PAD_Y * 2;
            const isSelected = row.id === selectedId;
            const label = row.operator.node_info.name;

            const bars = buildBars(
              row.points,
              globalPeakBuffer,
              innerHeight,
              innerY,
              row.lifetimeEndOffsetSec,
            );

            return (
              <g
                key={row.id}
                className="cursor-pointer"
                onClick={() => handleSelect(row.id)}
              >
                {/* Row background */}
                <rect
                  x={0}
                  y={rowY - ROW_GAP / 2}
                  width={LABEL_WIDTH + chartWidth}
                  height={ROW_HEIGHT + ROW_GAP}
                  fill={isSelected ? "rgba(255, 255, 255, 0.04)" : "transparent"}
                />

                {/* Row baseline */}
                <line
                  x1={LABEL_WIDTH}
                  y1={innerY + innerHeight}
                  x2={LABEL_WIDTH + chartWidth}
                  y2={innerY + innerHeight}
                  stroke="rgb(39, 39, 42)"
                  strokeWidth={1}
                />

                {/* Operator label */}
                <text
                  x={12}
                  y={rowY + ROW_HEIGHT / 2 + 4}
                  fill="rgb(228, 228, 231)"
                  fontSize={12}
                  fontFamily="ui-monospace, monospace"
                  className="select-none"
                >
                  {label.length > 26 ? `${label.slice(0, 25)}…` : label}
                </text>

                {/* Buffer-depth bars */}
                {bars.map((bar, bi) => (
                  <rect
                    key={bi}
                    x={bar.x}
                    y={bar.y}
                    width={bar.width}
                    height={bar.height}
                    fill={BAR_FILL_COLOR}
                  />
                ))}

                <title>
                  {label}
                  {"\n"}
                  {getStatusText(row.operator.status)} · peak buffer {formatBufferValue(row.peakBuffer)} {bufferLabel}
                  {"\n"}
                  {formatCount(row.finalRows)} rows out over {formatDuration(row.lifetimeEndOffsetSec - row.lifetimeStartOffsetSec)}
                </title>
              </g>
            );
          })}
        </svg>
          </div>

          {/* Right: sticky summary column (outside horizontal scroll) */}
          <div
            className="flex-shrink-0 border-l border-zinc-800 bg-zinc-900"
            style={{ width: SUMMARY_WIDTH }}
          >
            <svg
              width={SUMMARY_WIDTH}
              height={svgHeight}
              className="block"
            >
              {/* Header spacer + process track background to match the main svg */}
              {hasOverlay && (
                <>
                  <rect
                    x={0}
                    y={AXIS_HEIGHT}
                    width={SUMMARY_WIDTH}
                    height={trackHeight}
                    fill="rgb(17, 17, 19)"
                  />
                  <line
                    x1={0}
                    y1={AXIS_HEIGHT + trackHeight}
                    x2={SUMMARY_WIDTH}
                    y2={AXIS_HEIGHT + trackHeight}
                    stroke="rgb(39, 39, 42)"
                    strokeWidth={1}
                  />
                </>
              )}
              {rows.map((row, i) => {
                const rowY = rowsTop + i * (ROW_HEIGHT + ROW_GAP);
                const isSelected = row.id === selectedId;
                return (
                  <g
                    key={row.id}
                    className="cursor-pointer"
                    onClick={() => handleSelect(row.id)}
                  >
                    <rect
                      x={0}
                      y={rowY - ROW_GAP / 2}
                      width={SUMMARY_WIDTH}
                      height={ROW_HEIGHT + ROW_GAP}
                      fill={
                        isSelected
                          ? "rgba(255, 255, 255, 0.04)"
                          : "transparent"
                      }
                    />
                    <text
                      x={12}
                      y={rowY + ROW_HEIGHT / 2 + 4}
                      fill="rgb(161, 161, 170)"
                      fontSize={11}
                      fontFamily="ui-monospace, monospace"
                      className="select-none"
                    >
                      peak {formatBufferValue(row.peakBuffer)}
                    </text>
                    <text
                      x={SUMMARY_WIDTH - 12}
                      y={rowY + ROW_HEIGHT / 2 + 4}
                      fill="rgb(113, 113, 122)"
                      fontSize={11}
                      fontFamily="ui-monospace, monospace"
                      textAnchor="end"
                      className="select-none"
                    >
                      avg {formatBufferValue(Math.round(row.avgBuffer))}
                    </text>
                  </g>
                );
              })}
            </svg>
          </div>
        </div>
        </div>
        {topOps.length > 0 && (
          <div className="flex-shrink-0 border-t border-zinc-800 bg-zinc-950 px-4 py-2">
            <div
              className={`${main.className} text-[10px] uppercase tracking-wider text-zinc-500 mb-1.5`}
            >
              {metric === "cpu" ? "Hottest operators" : "Top bottlenecks"} · ranked by avg {bufferLabel}
              {" · click to inspect"}
            </div>
            <div className="space-y-0.5">
              {topOps.map((row, i) => {
                const isSelected = row.id === selectedId;
                return (
                  <button
                    key={row.id}
                    onClick={() => handleSelect(row.id)}
                    className={`${main.className} w-full flex items-center gap-4 text-left text-xs font-mono rounded px-2 py-1 transition-colors ${
                      isSelected
                        ? "bg-zinc-800 text-zinc-100"
                        : "text-zinc-400 hover:bg-zinc-900 hover:text-zinc-200"
                    }`}
                  >
                    <span className="text-zinc-600 w-4 flex-shrink-0">
                      {i + 1}
                    </span>
                    <span className="flex-1 truncate text-zinc-200">
                      {row.operator.node_info.name}
                    </span>
                    <span className="text-zinc-500 w-20 flex-shrink-0 text-right">
                      {row.operator.node_info.node_category}
                    </span>
                    <span className="w-28 flex-shrink-0 text-right">
                      peak {formatBufferValue(row.peakBuffer)}
                    </span>
                    <span className="w-28 flex-shrink-0 text-right">
                      avg {formatBufferValue(Math.round(row.avgBuffer))}
                    </span>
                    <span className="w-20 flex-shrink-0 text-right text-zinc-500">
                      {formatDuration(
                        row.lifetimeEndOffsetSec - row.lifetimeStartOffsetSec,
                      )}
                    </span>
                  </button>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {/* Details side panel */}
      {selectedRow && (
        <div className="w-80 border-l border-zinc-800 p-4 overflow-auto flex-shrink-0">
          <div className="flex items-start justify-between gap-2 mb-3">
            <div>
              <h3
                className={`${main.className} text-sm font-bold text-zinc-100 break-all`}
              >
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
            <Field
              label="Status"
              value={getStatusText(selectedRow.operator.status)}
            />
            <Field
              label={`Peak buffer (${bufferLabel})`}
              value={formatBufferValue(selectedRow.peakBuffer)}
            />
            <Field
              label={`Avg buffer (${bufferLabel})`}
              value={formatBufferValue(Math.round(selectedRow.avgBuffer))}
            />
            <Field
              label="Total rows out"
              value={selectedRow.finalRows.toLocaleString()}
            />
            <Field
              label="Duration"
              value={formatDuration(
                selectedRow.lifetimeEndOffsetSec - selectedRow.lifetimeStartOffsetSec,
              )}
            />
            <Field
              label="Started +"
              value={formatDuration(selectedRow.lifetimeStartOffsetSec)}
            />
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
      <dd
        className={`${main.className} text-zinc-200 font-mono text-right break-all`}
      >
        {value}
      </dd>
    </div>
  );
}
