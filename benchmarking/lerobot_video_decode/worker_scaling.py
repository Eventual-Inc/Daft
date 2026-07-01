"""Original vs batched decode, wall-vs-frames, one panel per worker count.

Reproduces the original-vs-batched frames sweep at 1/2/4/8 worker processes to show
that more workers do not let the per-frame version catch up: at every worker count
the original grows steeply with frame count (it re-opens and re-decodes from the
keyframe per frame) while batched stays low. Dense consecutive frames over local
shard copies, scalar return to isolate decode compute. Downloads a ~7MB shard first.

    python worker_scaling.py
"""

from __future__ import annotations

import json
import os
import shutil
import time
import urllib.request
from pathlib import Path

os.environ["DAFT_PROGRESS_BAR"] = "0"

import daft
from daft.datatype import DataType
from daft.functions import video_file
from daft.udf import func

HERE = Path(__file__).parent
CHARTS = HERE / "charts"
SHARD = HERE / "shard.mp4"
SHARD_URL = (
    "https://huggingface.co/datasets/pepijn223/egodex-test/resolve/main/"
    "videos/observation.image/chunk-000/file-000.mp4"
)
SHARDS = 8
FPS = 30
FRAMES_PER_SHARD = [5, 10, 20, 30]  # x-axis: total frames = SHARDS * this
WORKERS = [1, 2, 4, 8]
TOL = 1.0 / FPS / 2.0


@func(return_dtype=DataType.float64(), use_process=True)
def orig_mean(file, frame_ts, tol):
    """Original per-row behavior: reopen + seek-to-keyframe + decode forward, per frame."""
    import av

    abs_ts = float(frame_ts)
    tail = max(0.1, float(tol) * 50.0, 1.0 / 24.0)
    best = (1e9, 0.0)
    with file.open() as f_open, av.open(f_open) as c:
        st = c.streams.video[0]
        c.seek(max(0, int(abs_ts * av.time_base)), backward=True)
        for vf in c.decode(st):
            if vf.pts is None:
                continue
            cts = float(vf.pts * st.time_base)
            d = abs(cts - abs_ts)
            if d < best[0]:
                best = (d, float(vf.to_ndarray(format="rgb24").mean()))
            if cts >= abs_ts + tail:
                break
    return best[1]


@func.batch(return_dtype=DataType.float64(), use_process=True)
def batched_mean(files, frame_col, tol):
    """Batched: group rows by shard, decode the span once."""
    import av

    fl = files.to_pylist()
    fm = frame_col.to_pylist()
    by = {}
    sf = {}
    for i, f in enumerate(fl):
        by.setdefault(f.path, []).append((i, float(fm[i])))
        sf[f.path] = f
    res = [0.0] * len(fl)
    for path, targets in by.items():
        earliest = min(t for _, t in targets)
        latest = max(t for _, t in targets)
        best = {r: (1e9, 0.0) for r, _ in targets}
        with sf[path].open() as f_open, av.open(f_open) as c:
            st = c.streams.video[0]
            c.seek(max(0, int(earliest * av.time_base)), backward=True)
            for vf in c.decode(st):
                if vf.pts is None:
                    continue
                cts = float(vf.pts * st.time_base)
                m = float(vf.to_ndarray(format="rgb24").mean())
                for r, tg in targets:
                    d = abs(cts - tg)
                    if d < best[r][0]:
                        best[r] = (d, m)
                if cts >= latest + max(0.1, float(tol) * 50.0):
                    break
        for r, _ in targets:
            res[r] = best[r][1]
    return res


def ensure_shards():
    if not SHARD.exists():
        print(f"downloading shard -> {SHARD}")
        urllib.request.urlretrieve(SHARD_URL, SHARD)
    out = []
    for i in range(SHARDS):
        p = HERE / f"shard_{i}.mp4"
        if not p.exists():
            shutil.copy(SHARD, p)
        out.append(str(p))
    return out


def build_df(shards, frames_per_shard):
    urls, frame_vals = [], []
    for s in shards:
        for i in range(frames_per_shard):
            urls.append(s)
            frame_vals.append(i / FPS)
    df = daft.from_pydict({"url": urls, "frame": frame_vals})
    return df.with_column("v", video_file(df["url"])).into_batches(frames_per_shard)


def run(kind, workers, shards, frames_per_shard):
    df = build_df(shards, frames_per_shard)
    fn = (orig_mean if kind == "orig" else batched_mean).with_concurrency(workers)
    df = df.with_column("m", fn(df["v"], df["frame"], TOL))
    start = time.perf_counter()
    df.select("m").collect()
    return time.perf_counter() - start


def main():
    shards = ensure_shards()
    data = {}
    for w in WORKERS:
        orig, batched = [], []
        for fpp in FRAMES_PER_SHARD:
            orig.append(run("orig", w, shards, fpp))
            batched.append(run("batched", w, shards, fpp))
        data[w] = {"orig": orig, "batched": batched}
        print(f"workers={w}: orig={[round(x, 1) for x in orig]} batched={[round(x, 1) for x in batched]}", flush=True)

    (HERE / "worker_scaling.json").write_text(json.dumps({"fpp": FRAMES_PER_SHARD, "shards": SHARDS, "data": data}))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    x = [f * SHARDS for f in FRAMES_PER_SHARD]
    ymax = max(max(v["orig"]) for v in data.values()) * 1.1
    fig, axes = plt.subplots(2, 2, figsize=(11, 8), sharex=True)
    for ax, w in zip(axes.flat, WORKERS):
        ax.plot(x, data[w]["orig"], "o-", color="#d62728", label="original")
        ax.plot(x, data[w]["batched"], "s-", color="#2ca02c", label="batched")
        ax.set_title(f"{w} worker" + ("s" if w > 1 else ""))
        ax.set_ylim(0, ymax)
        ax.grid(True, alpha=0.3)
        ax.legend()
        ax.set_xlabel("total frames")
        ax.set_ylabel("wall time (s)")
    fig.suptitle(f"Original vs batched decode, by worker count ({SHARDS} shards)", fontsize=13)
    fig.tight_layout()
    CHARTS.mkdir(exist_ok=True)
    fig.savefig(CHARTS / "chart_workers.png", dpi=130)
    print("wrote charts/chart_workers.png")


if __name__ == "__main__":
    main()
