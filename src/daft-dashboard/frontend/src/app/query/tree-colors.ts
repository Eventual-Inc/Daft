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

/**
 * Heatmap style for an execution node, encoding bottleneck intensity as color.
 * intensity ∈ [0, 1] — 0 is "idle" (blends with background), 1 is "hot" (bottleneck).
 * Three-stop gradient through dusty/desaturated warm tones — reads as "slow",
 * not "error".
 */
export function getHeatmapStyle(intensity: number): {
  backgroundColor: string;
  borderColor: string;
} {
  const t = Math.max(0, Math.min(1, intensity));
  const cold: [number, number, number] = [24, 24, 27];
  const warm: [number, number, number] = [70, 48, 28];
  const hot: [number, number, number] = [110, 58, 42];

  const lerp = (a: number, b: number, u: number) => a + (b - a) * u;
  const mix = (a: [number, number, number], b: [number, number, number], u: number) =>
    [lerp(a[0], b[0], u), lerp(a[1], b[1], u), lerp(a[2], b[2], u)] as const;

  const rgb = t < 0.5 ? mix(cold, warm, t * 2) : mix(warm, hot, (t - 0.5) * 2);
  const [r, g, b] = rgb.map(Math.round);

  return {
    backgroundColor: `rgb(${r}, ${g}, ${b})`,
    borderColor: `rgb(${Math.min(255, r + 45)}, ${Math.min(255, g + 45)}, ${Math.min(255, b + 45)})`,
  };
}

/** Discrete style for a Finished operator — overrides the heatmap. */
export const FINISHED_STYLE: { backgroundColor: string; borderColor: string } = {
  backgroundColor: "rgb(20, 83, 45)",
  borderColor: "rgb(34, 122, 64)",
};

/**
 * Style for an amplification badge on an edge.
 * Cyan for expansion (>1×), purple for reduction (<1×), neutral otherwise.
 */
export function getAmplificationStyle(amplification: number): {
  borderColor: string;
  textColor: string;
} {
  if (amplification > 1) {
    return {
      borderColor: "rgb(34, 211, 238)",
      textColor: "rgb(165, 243, 252)",
    };
  }
  if (amplification > 0 && amplification < 1) {
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
