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

// Node types backed by a SpillReporter on the Rust side. Keep in sync with
// sinks that call record_bytes_written / set_in_memory_buffer_bytes.
export const SPILL_CAPABLE_NODE_TYPES = new Set<string>([
  "Aggregate",
  "GroupByAgg",
  "Repartition",
  "Sort",
  "TopN",
]);

export function hasSpillStrips(nodeType: string | undefined): boolean {
  return nodeType != null && SPILL_CAPABLE_NODE_TYPES.has(nodeType);
}

const COLORS = {
  memory: "rgb(217, 70, 219)",
  diskWritten: "rgb(217, 70, 219)",
  diskRead: "rgb(120, 50, 110)",
} as const;

type RateSample = { bytes: number; t: number };

// The dashboard has no fixed poll interval (SSE push + SWR), so rate is
// computed from the wall-clock delta between successive observations.
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
  color,
}: {
  label: string;
  fill: number;
  value: string;
  color: string;
}) {
  const pct = Math.max(0, Math.min(1, fill)) * 100;
  return (
    <div className="flex items-center gap-2 text-[10px]">
      <span
        className={`${main.className} uppercase tracking-wider text-zinc-500 w-12 shrink-0`}
      >
        {label}
      </span>
      <div className="flex-1 h-1.5 rounded-sm bg-zinc-800/80 overflow-hidden min-w-0">
        <div
          className="h-full rounded-sm"
          style={{ width: `${pct}%`, backgroundColor: color }}
        />
      </div>
      <span
        className={`${main.className} text-xs text-zinc-300 font-mono tabular-nums
          w-20 shrink-0 text-right whitespace-nowrap`}
      >
        {value}
      </span>
    </div>
  );
}

export default function SpillStrips({ operator }: { operator?: OperatorInfo }) {
  // Latches true once we've seen the in-memory-buffer gauge at least once.
  // Sinks that never call set_in_memory_buffer_bytes (e.g. Flight-path
  // Repartition) would otherwise render a misleading flat-at-zero mem bar.
  const memObserved = useRef(false);
  const memKeyPresent =
    operator?.stats[IN_MEMORY_BUFFER_BYTES_STAT_KEY] !== undefined;
  if (memKeyPresent) memObserved.current = true;

  const memBytes = statNumericValue(
    operator?.stats[IN_MEMORY_BUFFER_BYTES_STAT_KEY]
  );
  const bytesWritten = statNumericValue(
    operator?.stats[SPILL_BYTES_WRITTEN_STAT_KEY]
  );
  const bytesRead = statNumericValue(
    operator?.stats[SPILL_BYTES_READ_STAT_KEY]
  );
  const filesResident = statNumericValue(
    operator?.stats[SPILL_FILES_RESIDENT_STAT_KEY]
  );

  const writeRate = useSpillWriteRate(
    bytesWritten,
    operator?.status === "Executing"
  );

  if (!operator) return null;
  if (memBytes <= 0 && bytesWritten <= 0 && filesResident <= 0) return null;

  // Shared scale so the gap between written and read visualizes resident spill.
  const diskScale = Math.max(bytesWritten, bytesRead, 1);
  const memScale = Math.max(memBytes, 1);

  const rateStr = formatByteRate(writeRate);
  const diskSubParts = [
    filesResident > 0 ? `${filesResident.toLocaleString()} files` : null,
    rateStr ? `w +${rateStr}` : null,
  ].filter(Boolean);

  return (
    <div className="mt-2 pt-2 border-t border-zinc-700/50 space-y-1.5">
      {memObserved.current && (
        <StripBar
          label="mem"
          fill={memBytes / memScale}
          value={formatBytes(memBytes)}
          color={COLORS.memory}
        />
      )}
      <div className="pt-1 space-y-1">
        <div
          className={`${main.className} text-[10px] uppercase tracking-wider text-zinc-500`}
        >
          disk
        </div>
        <StripBar
          label="▲ wrote"
          fill={bytesWritten / diskScale}
          value={formatBytes(bytesWritten)}
          color={COLORS.diskWritten}
        />
        <StripBar
          label="▼ read"
          fill={bytesRead / diskScale}
          value={formatBytes(bytesRead)}
          color={COLORS.diskRead}
        />
        {diskSubParts.length > 0 && (
          <div
            className={`${main.className} pl-12 text-[10px] text-zinc-500 font-mono
              whitespace-nowrap overflow-hidden text-ellipsis`}
          >
            {diskSubParts.join(" · ")}
          </div>
        )}
      </div>
    </div>
  );
}
