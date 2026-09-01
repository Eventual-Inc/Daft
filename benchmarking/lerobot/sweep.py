"""Sweep the decode over row counts and chart wall time.

Run once on each build to compare. Writes results JSON + a chart. If both
`sweep_original.json` and `sweep_batched.json` exist, also emits the old-vs-new
overlay used in the README.

    python sweep.py --label batched     # tags the output file
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

os.environ["DAFT_PROGRESS_BAR"] = "0"

DATASET = "pepijn223/egodex-test"
IMAGE_COLUMN = "observation.image"
ROWS = list(range(1, 11))
HERE = Path(__file__).parent
CHARTS = HERE / "charts"


def decode_rows(n: int) -> float:
    from daft.datasets import lerobot

    df = lerobot.read(DATASET, load_video_frames=IMAGE_COLUMN).limit(n)
    start = time.perf_counter()
    df.select("episode_index", "frame_index", IMAGE_COLUMN).collect()
    return time.perf_counter() - start


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="current", help="tag for the results file (e.g. original / batched)")
    args = parser.parse_args()

    import daft

    print(f"daft {daft.__version__}, label={args.label}, rows {ROWS}", flush=True)
    results = []
    for n in ROWS:
        wall = decode_rows(n)
        results.append({"rows": n, "wall": wall})
        print(f"rows={n:2d}  wall={wall:6.2f}s  ({wall / n:5.2f}s/frame)", flush=True)
    (HERE / f"sweep_{args.label}.json").write_text(json.dumps(results, indent=2))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    orig = HERE / "sweep_original.json"
    batched = HERE / "sweep_batched.json"
    if orig.exists() and batched.exists():
        o = json.loads(orig.read_text())
        b = json.loads(batched.read_text())
        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.plot([r["rows"] for r in o], [r["wall"] for r in o], "o-", color="#d62728", label="original (open per frame)")
        ax.plot([r["rows"] for r in b], [r["wall"] for r in b], "s-", color="#2ca02c", label="batched (open per shard)")
        ax.set_title("LeRobot decode: original vs batched")
        ax.set_xlabel("frames decoded")
        ax.set_ylabel("wall time (s)")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        CHARTS.mkdir(exist_ok=True)
        fig.savefig(CHARTS / "chart_old_vs_new.png", dpi=130)
        print("wrote charts/chart_old_vs_new.png", flush=True)


if __name__ == "__main__":
    main()
