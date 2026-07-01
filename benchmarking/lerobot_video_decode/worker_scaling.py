"""Original vs batched decode across worker counts.

Answers: can more workers let the per-frame version catch up? No - the original
re-opens and re-decodes from the keyframe for every frame, and parallelism only
spreads that redundant work. Uses dense consecutive frames (the real LeRobot case)
over local shard copies, and returns a scalar per row so decode compute (not image
serialization) is what is measured. Downloads a ~7MB shard on first run.

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
NFRAMES = 30  # dense consecutive frames per shard
TOL = 1.0 / FPS / 2.0
WORKERS = [1, 2, 4, 8]


@func(return_dtype=DataType.float64(), use_process=True)
def orig_mean(file, from_ts, frame_ts, tol):
    """Original per-row behavior: reopen + seek-to-keyframe + decode forward, per frame."""
    import av

    abs_ts = float(from_ts) + float(frame_ts)
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
def batched_mean(files, from_col, frame_col, tol):
    """Batched: group rows by shard, decode the span once."""
    import av

    fl = files.to_pylist()
    fr = from_col.to_pylist()
    fm = frame_col.to_pylist()
    by = {}
    sf = {}
    for i, f in enumerate(fl):
        by.setdefault(f.path, []).append((i, float(fr[i]) + float(fm[i])))
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


def build_df(shards):
    urls, from_vals, frame_vals = [], [], []
    for s in shards:
        for i in range(NFRAMES):
            urls.append(s)
            from_vals.append(0.0)
            frame_vals.append(i / FPS)
    df = daft.from_pydict({"url": urls, "from": from_vals, "frame": frame_vals})
    return df.with_column("v", video_file(df["url"])).into_batches(NFRAMES)


def run(kind, workers, shards):
    df = build_df(shards)
    fn = (orig_mean if kind == "orig" else batched_mean).with_concurrency(workers)
    df = df.with_column("m", fn(df["v"], df["from"], df["frame"], TOL))
    start = time.perf_counter()
    df.select("m").collect()
    return time.perf_counter() - start


def main():
    shards = ensure_shards()
    print(f"{SHARDS} shards x {NFRAMES} consecutive frames = {SHARDS * NFRAMES} rows\n")
    print(f"{'workers':>7}  {'original':>9}  {'batched':>9}  {'ratio':>6}")
    orig, batched = [], []
    for w in WORKERS:
        o = run("orig", w, shards)
        b = run("batched", w, shards)
        orig.append(o)
        batched.append(b)
        print(f"{w:>7}  {o:>8.2f}s  {b:>8.2f}s  {o / b:>5.1f}x", flush=True)

    (HERE / "worker_scaling.json").write_text(json.dumps({"workers": WORKERS, "orig": orig, "batched": batched}))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(WORKERS, orig, "o-", color="#d62728", label="original (open+decode per frame)")
    ax.plot(WORKERS, batched, "s-", color="#2ca02c", label="batched (per shard)")
    ax.set_title(f"Original vs batched across worker processes\n({SHARDS} shards x {NFRAMES} consecutive frames)")
    ax.set_xlabel("worker processes")
    ax.set_ylabel("wall time (s)")
    ax.set_xticks(WORKERS)
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    CHARTS.mkdir(exist_ok=True)
    fig.savefig(CHARTS / "chart_workers.png", dpi=130)
    print("\nwrote charts/chart_workers.png")


if __name__ == "__main__":
    main()
