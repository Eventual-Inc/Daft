"""Row-order effect on grouping, as the dataset scales up.

Grouping happens per batch, so the number of opens depends on whether a shard's rows
land in the same batch. Counts av.open() calls for ordered vs shuffled rows vs the
original (one open per frame) as the number of shards grows. Local shard copies.
Downloads a ~7MB shard on first run.

    python ordering.py
"""

from __future__ import annotations

import json
import os
import random
import shutil
import urllib.request
from pathlib import Path

os.environ["DAFT_PROGRESS_BAR"] = "0"

import av

import daft
from daft.datasets.lerobot import _decode_lerobot_video_timestamp as decode
from daft.functions import video_file

HERE = Path(__file__).parent
CHARTS = HERE / "charts"
SHARD = HERE / "shard.mp4"
SHARD_URL = (
    "https://huggingface.co/datasets/pepijn223/egodex-test/resolve/main/"
    "videos/observation.image/chunk-000/file-000.mp4"
)
FPS = 30
FRAMES_PER_SHARD = 8
BATCH = 16
SHARD_COUNTS = [2, 4, 8, 16]
TOL = 1.0 / FPS / 2.0

_orig_open = av.open
_opens = {"n": 0}
av.open = lambda *a, **k: (_opens.__setitem__("n", _opens["n"] + 1) or _orig_open(*a, **k))


def ensure_shards(k):
    if not SHARD.exists():
        print(f"downloading shard -> {SHARD}")
        urllib.request.urlretrieve(SHARD_URL, SHARD)
    out = []
    for i in range(k):
        p = HERE / f"shard_{i}.mp4"
        if not p.exists():
            shutil.copy(SHARD, p)
        out.append(str(p))
    return out


def opens_for(rows):
    _opens["n"] = 0
    df = daft.from_pydict({"u": [r[0] for r in rows], "f": [0.0] * len(rows), "t": [r[1] for r in rows]})
    df = df.with_column("v", video_file(df["u"])).into_batches(BATCH)
    df = df.with_column("img", decode(df["v"], df["f"], df["t"], TOL, 0, 0))
    df.select("img").collect()
    return _opens["n"]


def main():
    shards = ensure_shards(max(SHARD_COUNTS))
    xs, ordered, shuffled, original = [], [], [], []
    rng = random.Random(0)
    for nshards in SHARD_COUNTS:
        rows = [(shards[s], j / FPS) for s in range(nshards) for j in range(FRAMES_PER_SHARD)]
        o = opens_for(rows)
        sh = rows[:]
        rng.shuffle(sh)
        s = opens_for(sh)
        xs.append(len(rows))
        ordered.append(o)
        shuffled.append(s)
        original.append(len(rows))
        print(f"rows={len(rows):3d} ({nshards} shards)  ordered={o:3d}  shuffled={s:3d}  original={len(rows):3d}", flush=True)

    (HERE / "ordering.json").write_text(json.dumps({"rows": xs, "ordered": ordered, "shuffled": shuffled}))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(xs, original, "o-", color="#d62728", label="original (open per frame)")
    ax.plot(xs, shuffled, "^-", color="#ff7f0e", label="batched, shuffled rows")
    ax.plot(xs, ordered, "s-", color="#2ca02c", label="batched, ordered rows")
    ax.set_title("Opens vs dataset size (8 frames/shard, batch=16)")
    ax.set_xlabel("total rows (frames)")
    ax.set_ylabel("av.open() calls")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    CHARTS.mkdir(exist_ok=True)
    fig.savefig(CHARTS / "chart_ordering.png", dpi=130)
    print("wrote charts/chart_ordering.png")


if __name__ == "__main__":
    main()
