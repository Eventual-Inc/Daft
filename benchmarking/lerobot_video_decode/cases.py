"""Case tests for the batched decode: batch-size effect + multi-shard grouping.

Runs the real batch UDF over local copies of a shard, counting av.open() calls -
the metric that matters, since each open is one network fetch on a remote shard.
Downloads a small (~7MB) shard on first run.

    python cases.py
"""

from __future__ import annotations

import os
import shutil
import urllib.request
from pathlib import Path

os.environ["DAFT_PROGRESS_BAR"] = "0"

import av
import numpy as np

import daft
from daft.datasets.lerobot import _decode_lerobot_video_timestamp
from daft.functions import video_file

HERE = Path(__file__).parent
CHARTS = HERE / "charts"
SHARD = HERE / "shard.mp4"
SHARD_URL = (
    "https://huggingface.co/datasets/pepijn223/egodex-test/resolve/main/"
    "videos/observation.image/chunk-000/file-000.mp4"
)
TOL = 1.0 / 30.0 / 2.0

# instrument av.open to count opens
_orig_open = av.open
_opens = {"n": 0}


def _counting_open(*a, **k):
    _opens["n"] += 1
    return _orig_open(*a, **k)


av.open = _counting_open


def ensure_shards(k):
    if not SHARD.exists():
        print(f"downloading shard -> {SHARD}")
        urllib.request.urlretrieve(SHARD_URL, SHARD)
    paths = []
    for i in range(k):
        p = HERE / f"shard_{i}.mp4"
        if not p.exists():
            shutil.copy(SHARD, p)
        paths.append(str(p))
    return paths


def run(urls, tss, batch):
    _opens["n"] = 0
    df = daft.from_pydict({"url": urls, "from_ts": [0.0] * len(urls), "ts": tss})
    df = df.with_column("video", video_file(df["url"]))
    df = df.into_batches(batch)
    df = df.with_column("img", _decode_lerobot_video_timestamp(df["video"], df["from_ts"], df["ts"], TOL, 0, 0))
    out = df.select("img").to_pydict()["img"]
    return _opens["n"], out


def main() -> None:
    ensure_shards(1)

    print("=== Case 1: batch-size sweep (1 shard, 32 rows) ===")
    print("opens should = number of batches = ceil(32 / batch_size)\n")
    urls1 = [str(SHARD)] * 32
    tss1 = [i / 30.0 for i in range(32)]
    batch_rows = []
    for b in [1, 2, 4, 8, 16, 32]:
        opens, out = run(urls1, tss1, b)
        batch_rows.append((b, opens))
        print(f"  batch={b:3d}  opens={opens:3d}  rows={len(out)}")

    print("\n=== Case 2: multi-shard (5 shards, 40 rows, batch=16) ===")
    print("interleaved vs grouped-by-shard row order\n")
    shards = ensure_shards(5)
    ts5 = [i / 30.0 for i in range(8)]
    inter_urls, inter_ts = [], []
    for j in range(8):
        for s in shards:
            inter_urls.append(s)
            inter_ts.append(ts5[j])
    grp_urls, grp_ts = [], []
    for s in shards:
        for j in range(8):
            grp_urls.append(s)
            grp_ts.append(ts5[j])

    opens_i, out_i = run(inter_urls, inter_ts, 16)
    opens_g, _ = run(grp_urls, grp_ts, 16)
    print(f"  interleaved order:  opens={opens_i:3d}")
    print(f"  grouped-by-shard:   opens={opens_g:3d}")

    seen = {}
    ok = True
    for t, im in zip(inter_ts, out_i):
        b = np.asarray(im).tobytes()
        if t in seen and seen[t] != b:
            ok = False
        seen[t] = b
    print(f"  correctness (same frame per ts across shard copies): {'OK' if ok else 'MISMATCH'}")

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
    ax1.plot([r[0] for r in batch_rows], [r[1] for r in batch_rows], "o-", color="#1f77b4")
    ax1.set_title("Case 1: opens vs batch size (1 shard, 32 rows)")
    ax1.set_xlabel("batch size (into_batches)")
    ax1.set_ylabel("av.open() calls")
    ax1.grid(True, alpha=0.3)
    ax2.bar(["interleaved", "grouped\nby shard"], [opens_i, opens_g], color=["#d62728", "#2ca02c"])
    ax2.set_title("Case 2: opens, 5 shards / 40 rows / batch=16")
    ax2.set_ylabel("av.open() calls")
    for i, v in enumerate([opens_i, opens_g]):
        ax2.text(i, v + 0.3, str(v), ha="center")
    fig.tight_layout()
    CHARTS.mkdir(exist_ok=True)
    fig.savefig(CHARTS / "chart_cases.png", dpi=130)
    print("\nwrote charts/chart_cases.png")


if __name__ == "__main__":
    main()
