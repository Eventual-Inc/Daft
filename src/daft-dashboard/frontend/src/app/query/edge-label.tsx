"use client";

import { CSSProperties } from "react";
import { main } from "@/lib/utils";
import { formatBytes } from "./stats-utils";
import { getAmplificationStyle } from "./tree-colors";

const MIN_THICKNESS_PX = 1;
const MAX_THICKNESS_PX = 14;
const EXPANSION_THRESHOLD = 2;
const REDUCTION_THRESHOLD = 0.5;

const computeThickness = (bytes: number, maxBytes: number) => {
  if (maxBytes <= 0 || bytes <= 0) return MIN_THICKNESS_PX;
  return (
    MIN_THICKNESS_PX +
    (MAX_THICKNESS_PX - MIN_THICKNESS_PX) * Math.sqrt(Math.min(1, bytes / maxBytes))
  );
};

const formatMultiplier = (amp: number): string => {
  if (!isFinite(amp) || amp <= 0) return "";
  if (amp >= 1000) return `×${(amp / 1000).toFixed(amp >= 10000 ? 0 : 1)}k`;
  if (amp >= 10) return `×${amp.toFixed(0)}`;
  if (amp >= 2) return `×${amp.toFixed(1)}`;
  return `×${amp.toFixed(2)}`;
};

type Position = "single" | "branch";

type EdgeLabelProps = {
  bytes: number;
  amplification: number;
  maxBytes: number;
  position: Position;
};

export default function EdgeLabel({
  bytes,
  amplification,
  maxBytes,
  position,
}: EdgeLabelProps) {
  const thickness = computeThickness(bytes, maxBytes);
  const isExpansion = amplification >= EXPANSION_THRESHOLD;
  const isReduction = amplification > 0 && amplification <= REDUCTION_THRESHOLD;
  const showBadge = (isExpansion || isReduction) && bytes > 0;
  const style = showBadge ? getAmplificationStyle(amplification) : null;

  const lineStyle: CSSProperties = {
    width: `${thickness}px`,
    backgroundColor: "rgb(82, 82, 91)",
  };

  const stubClass = position === "branch" ? "h-2" : "h-3";

  return (
    <div className="flex flex-col items-center" data-edge-position={position}>
      <div className={stubClass} style={lineStyle} />
      {showBadge && style && (
        <div
          className={`${main.className} my-0.5 px-2 py-0.5 rounded border bg-zinc-900/85 leading-tight flex items-center gap-2`}
          style={{ borderColor: style.borderColor }}
        >
          <span
            className="text-[10px] font-mono font-bold whitespace-nowrap"
            style={{ color: style.textColor }}
          >
            {formatMultiplier(amplification)} {isExpansion ? "↗" : "↘"}
          </span>
          <span className="text-[10px] font-mono text-zinc-400 whitespace-nowrap">
            {formatBytes(bytes)}
          </span>
        </div>
      )}
      <div className={stubClass} style={lineStyle} />
    </div>
  );
}
