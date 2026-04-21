"use client";

import { useEffect, useRef, useState } from "react";
import { main } from "@/lib/utils";
import { OperatorInfo } from "./types";
import {
  IN_MEMORY_BUFFER_BYTES_STAT_KEY,
  SPILL_BYTES_READ_STAT_KEY,
  SPILL_BYTES_WRITTEN_STAT_KEY,
  SPILL_FILES_RESIDENT_STAT_KEY,
  formatBytes,
  formatByteRate,
  statNumericValue,
} from "./stats-utils";

/**
 * Node types that behave as global aggregates — blocking sinks that
 * accumulate all input before emitting and can spill their working set
 * to disk. Keep in sync with the Rust side: any sink using `SpillReporter`
 * + `set_in_memory_buffer_bytes` should be listed here.
 */
export const GLOBAL_AGG_NODE_TYPES = new Set<string>([
  "Aggregate",
  "GroupedAggregate",
  "Repartition",
  "Sort",
  "TopN",
]);

export function hasSpillStrips(nodeType: string | undefined): boolean {
  return nodeType != null && GLOBAL_AGG_NODE_TYPES.has(nodeType);
}

type RateSample = { bytes: number; t: number };

/**
 * Track per-operator write rate across poll ticks. The dashboard has no
 * fixed refresh interval (SSE push + 5s SWR), so we diff against the last
 * sample we observed and compute bytes/sec from wall-clock delta.
 */
function useSpillWriteRate(bytesWritten: number, isExecuting: boolean): number {
  const prev = useRef<RateSample | null>(null);
  const [rate, setRate] = useState(0);

  useEffect(() => {
    if (!isExecuting) {
      prev.current = null;
      return;
    }
    const now = Date.now() / 1000;
    const last = prev.current;
    if (last && now > last.t && bytesWritten >= last.bytes) {
      setRate((bytesWritten - last.bytes) / (now - last.t));
    }
    prev.current = { bytes: bytesWritten, t: now };
  }, [bytesWritten, isExecuting]);

  return rate;
}

function StripBar({
  label,
  fill,
  value,
  sub,
  color,
}: {
  label: string;
  fill: number;
  value: string;
  sub?: string;
  color: string;
}) {
  const pct = Math.max(0, Math.min(1, fill)) * 100;
  return (
    <div className="flex flex-col gap-0.5">
      <div className="flex items-center gap-2 text-[10px]">
        <span
          className={`${main.className} uppercase tracking-wider text-zinc-500 w-10 shrink-0`}
        >
          {label}
        </span>
        <div className="flex-1 h-1.5 rounded-sm bg-zinc-800/80 overflow-hidden">
          <div
            className="h-full rounded-sm transition-[width] duration-500 ease-out"
            style={{ width: `${pct}%`, backgroundColor: color }}
          />
        </div>
        <span
          className={`${main.className} text-xs text-zinc-300 font-mono tabular-nums`}
        >
          {value}
        </span>
      </div>
      {sub && (
        <div
          className={`${main.className} pl-12 text-[10px] text-zinc-500 font-mono`}
        >
          {sub}
        </div>
      )}
    </div>
  );
}

export default function SpillStrips({ operator }: { operator?: OperatorInfo }) {
  const memBytes = operator
    ? statNumericValue(operator.stats[IN_MEMORY_BUFFER_BYTES_STAT_KEY])
    : 0;
  const bytesWritten = operator
    ? statNumericValue(operator.stats[SPILL_BYTES_WRITTEN_STAT_KEY])
    : 0;
  const bytesRead = operator
    ? statNumericValue(operator.stats[SPILL_BYTES_READ_STAT_KEY])
    : 0;
  const filesResident = operator
    ? statNumericValue(operator.stats[SPILL_FILES_RESIDENT_STAT_KEY])
    : 0;

  const diskHeld = Math.max(0, bytesWritten - bytesRead);
  const maxSeen = Math.max(memBytes, diskHeld, 1);

  const writeRate = useSpillWriteRate(
    bytesWritten,
    operator?.status === "Executing",
  );

  if (!operator) return null;

  // Don't render the block at all until there's something to show —
  // keeps streaming ops (that never touch the buffer gauge) visually clean.
  const hasAny = memBytes > 0 || bytesWritten > 0 || filesResident > 0;
  if (!hasAny) return null;

  const rateStr = formatByteRate(writeRate);
  const diskSub = [
    filesResident > 0 ? `${filesResident.toLocaleString()} files` : null,
    rateStr ? `+${rateStr}` : null,
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <div className="mt-2 pt-2 border-t border-zinc-700/50 space-y-1.5">
      <StripBar
        label="mem"
        fill={memBytes / maxSeen}
        value={formatBytes(memBytes)}
        color="rgb(217, 70, 219)"
      />
      <StripBar
        label="disk"
        fill={diskHeld / maxSeen}
        value={formatBytes(diskHeld)}
        sub={diskSub || undefined}
        color="rgb(120, 50, 110)"
      />
    </div>
  );
}
