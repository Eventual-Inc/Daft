export const categoryColors: Record<string, { bg: string; border: string; text: string }> = {
  Source: { bg: "bg-emerald-950", border: "border-emerald-700", text: "text-emerald-300" },
  Filter: { bg: "bg-amber-950", border: "border-amber-700", text: "text-amber-300" },
  Project: { bg: "bg-sky-950", border: "border-sky-700", text: "text-sky-300" },
  Sort: { bg: "bg-violet-950", border: "border-violet-700", text: "text-violet-300" },
  Join: { bg: "bg-rose-950", border: "border-rose-700", text: "text-rose-300" },
  Aggregate: { bg: "bg-fuchsia-950", border: "border-fuchsia-700", text: "text-fuchsia-300" },
  Sink: { bg: "bg-indigo-950", border: "border-indigo-700", text: "text-indigo-300" },
};

export const defaultColor = { bg: "bg-zinc-900", border: "border-zinc-600", text: "text-zinc-300" };

export type HeatmapStyle = {
  backgroundColor: string;
  borderColor: string;
  borderWidth: string;
};

/**
 * Heatmap style for an execution node, encoding bottleneck intensity as color.
 * intensity ∈ [0, 1] — 0 is "idle", 1 is "hot" (bottleneck).
 *
 * The border carries the real heat signal (dim grey → dusty mauve → Daft
 * magenta) and thickens as it heats up. The background only whispers a
 * magenta tint at high heat, keeping white text comfortably legible at every
 * intensity. Stays within the daft.ai palette (black + magenta + greys).
 */
export function getHeatmapStyle(intensity: number): HeatmapStyle {
  const t = Math.max(0, Math.min(1, intensity));

  const bgCold: [number, number, number] = [24, 24, 27];
  const bgHot: [number, number, number] = [30, 22, 32];

  const borderCold: [number, number, number] = [63, 63, 70];
  const borderWarm: [number, number, number] = [120, 50, 110];
  const borderHot: [number, number, number] = [217, 70, 219];

  const lerp = (a: number, b: number, u: number) => a + (b - a) * u;
  const mix = (a: [number, number, number], b: [number, number, number], u: number) =>
    [lerp(a[0], b[0], u), lerp(a[1], b[1], u), lerp(a[2], b[2], u)] as const;

  const bg = mix(bgCold, bgHot, t).map(Math.round);
  const border =
    t < 0.5
      ? mix(borderCold, borderWarm, t * 2).map(Math.round)
      : mix(borderWarm, borderHot, (t - 0.5) * 2).map(Math.round);

  return {
    backgroundColor: `rgb(${bg[0]}, ${bg[1]}, ${bg[2]})`,
    borderColor: `rgb(${border[0]}, ${border[1]}, ${border[2]})`,
    borderWidth: `${lerp(2, 4, t).toFixed(2)}px`,
  };
}

/** Discrete style for a Finished operator — overrides the heatmap.
 * Background fades to cold zinc so the eye isn't pulled toward done work;
 * a muted emerald border still marks it as completed (vs. Pending, which
 * is cold zinc with no special border). */
export const FINISHED_STYLE: HeatmapStyle = {
  backgroundColor: "rgb(24, 24, 27)",
  borderColor: "rgb(52, 120, 80)",
  borderWidth: "2px",
};

/**
 * Thresholds at which an edge's bytes-out / bytes-in ratio is considered
 * meaningful enough to badge. Shared with EdgeLabel so both files agree on
 * what counts as expansion vs. reduction.
 */
export const EXPANSION_THRESHOLD = 1.25;
export const REDUCTION_THRESHOLD = 0.8;

/**
 * Style for an amplification badge on an edge.
 * Cyan for expansion (≥ EXPANSION_THRESHOLD), purple for reduction
 * (≤ REDUCTION_THRESHOLD), neutral otherwise.
 */
export function getAmplificationStyle(amplification: number): {
  borderColor: string;
  textColor: string;
} {
  if (amplification >= EXPANSION_THRESHOLD) {
    return {
      borderColor: "rgb(34, 211, 238)",
      textColor: "rgb(165, 243, 252)",
    };
  }
  if (amplification > 0 && amplification <= REDUCTION_THRESHOLD) {
    return {
      borderColor: "rgb(192, 132, 252)",
      textColor: "rgb(233, 213, 255)",
    };
  }
  return {
    borderColor: "rgb(82, 82, 91)",
    textColor: "rgb(161, 161, 170)",
  };
}
