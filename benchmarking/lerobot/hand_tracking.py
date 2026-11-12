"""Time the daft-physical-ai hand-tracking demo workload end to end.

This is the downstream workload from the daft-physical-ai demo
(https://github.com/Eventual-Inc/daft-physical-ai/blob/main/examples/demo.py):
read a remote LeRobot dataset, decode 12 frames, run MediaPipe hand tracking
as a Daft UDF, and materialize the hands column. Run once per reader revision
(like real_datasets.py), then chart the pair:

    python hand_tracking.py <out.json>
    python hand_tracking.py --chart orig.json batched.json

Requires `pip install "daft-physical-ai[mediapipe]"` in addition to daft.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
CHARTS = HERE / "charts"

DATASET = "pepijn223/egodex-test"
IMAGE_COLUMN = "observation.image"
N_ROWS = 12


def chart(path_orig: str, path_batched: str) -> int:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    o, b = json.load(open(path_orig)), json.load(open(path_batched))
    if [r["n_hands"] for r in o["rows"]] != [r["n_hands"] for r in b["rows"]]:
        print("WARNING: detected hands differ between runs")

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar(
        ["original\n(open per frame)", "batched\n(open per shard)"],
        [o["wall"], b["wall"]],
        color=["#d62728", "#2ca02c"],
        width=0.55,
    )
    for bar, wall in zip(bars, [o["wall"], b["wall"]]):
        ax.text(bar.get_x() + bar.get_width() / 2, wall, f"{wall:.1f}s", ha="center", va="bottom")
    ax.set_ylabel("wall time (s)")
    ax.set_title(f"Hand-tracking workload: decode + MediaPipe\n{N_ROWS} remote frames, {DATASET}")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    CHARTS.mkdir(exist_ok=True)
    out = CHARTS / "chart_hand_tracking.png"
    fig.savefig(out, dpi=130)
    print(f"wrote {out.relative_to(HERE)}")
    return 0


def main() -> int:
    if sys.argv[1] == "--chart":
        return chart(sys.argv[2], sys.argv[3])

    from daft.datasets import lerobot
    from daft_physical_ai.hands import track_hands

    out_path = sys.argv[1]

    t0 = time.perf_counter()
    df = lerobot.read(DATASET, load_video_frames=IMAGE_COLUMN).limit(N_ROWS)
    df = df.with_column("hands", track_hands(df[IMAGE_COLUMN], method="mediapipe"))
    data = df.select("episode_index", "frame_index", "hands").to_pydict()
    wall = time.perf_counter() - t0

    rows = [
        {
            "episode_index": int(data["episode_index"][i]),
            "frame_index": int(data["frame_index"][i]),
            "n_hands": len(data["hands"][i] or []),
        }
        for i in range(len(data["episode_index"]))
    ]
    rows.sort(key=lambda r: (r["episode_index"], r["frame_index"]))
    assert len(rows) == N_ROWS, f"expected {N_ROWS} rows, got {len(rows)}"

    with open(out_path, "w") as f:
        json.dump({"wall": wall, "rows": rows}, f, indent=1, sort_keys=True)
    print(f"OK {DATASET}: {len(rows)} rows in {wall:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
