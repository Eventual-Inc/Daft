# Web Visualizations Summary

This folder contains interactive pages to explain how repartition works in Daft/Flotilla distributed execution.

## Files

- `web/index.html`
- `web/repartition-animation.html`
- `web/flotilla-repartition-flow.md`
- `web/flotilla-repartition-flow.html`

## `web/index.html` (Static + Interactive Snapshot)

Purpose:
- Quick visual model of repartition stages and outputs.
- Good for comparing repartition types side-by-side.

What it shows:
1. Upstream input tasks
2. Local repartition output per task
3. Transpose/merge groups (grouped by partition index)
4. Downstream tasks (one task per final partition)

Controls:
- Repartition type: `Hash`, `Random`, `Range`, `IntoPartitions`
- Hash column (only shown when `Hash` is selected)
- Input workers
- Output partitions
- Re-run button

Notable behavior:
- Row color is tied to origin input task (`T0`, `T1`, ...) to track rows across stages.
- `Hash` lets you switch hash key column (`user_id`, `score`, `id`).
- `IntoPartitions` is displayed as a separate behavior (concat + even slicing).

## `web/repartition-animation.html` (Step-by-step Animation)

Purpose:
- Animate row movement through the distributed operator pipeline.
- Show control-plane sequencing and data movement visually.

What it shows:
1. Rows enter upstream tasks
2. Local repartition per upstream task
3. Transpose into merge groups by partition index
4. Emission into downstream task per final partition

Controls:
- Type: `Hash`, `Random`, `Range`
- Hash column (only for hash)
- Workers
- Partitions
- `Start`, `Pause`, `Reset`, `Step`

Execution ordering in the animation:
- Phase-based (matches distributed code path more closely):
  - all input placement
  - local repartition materialization per task
  - transpose per partition index
  - downstream emission per final partition

## `web/flotilla-repartition-flow.md` (Mermaid Source)

Purpose:
- Source diagram of the distributed repartition flow.
- Useful for docs/review and easy edits.

Includes:
- `Hash/Random/Range` shuffle path
- optional pre-shuffle merge
- transpose/merge
- downstream task emission
- separate `IntoPartitions` path

## `web/flotilla-repartition-flow.html` (Standalone Diagram Page)

Purpose:
- Browser-rendered Mermaid version of the flow diagram.

Behavior:
- Loads Mermaid from CDN and renders the graph.
- Shows a fallback message if Mermaid cannot load.

## How to open

On macOS:

```bash
open web/index.html
open web/repartition-animation.html
open web/flotilla-repartition-flow.html
```

## Scope and simplifications

These pages are educational visualizations, not production tracing UIs.

- They model the repartition/dataflow semantics and task grouping.
- They do not show every scheduler detail, retry path, or memory management nuance.
- They intentionally simplify some internals for readability while preserving the core flow.
