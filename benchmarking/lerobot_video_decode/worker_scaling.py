"""Worker-scaling for the batched decode: multi-shard decode across N processes.

With multiple shards, the batched decode parallelizes across worker processes -
each process opens and decodes a different shard. To measure decode-compute
scaling (not image serialization), each row returns a scalar (mean pixel) rather
than the image, and each shard is decoded over its full span (~632 frames) to make
decode the dominant cost. Local shard copies, so this is CPU-decode parallelism,
not the network-fetch parallelism that also benefits on a real cluster.

    python worker_scaling.py
"""

from __future__ import annotations

import os
import shutil
import time
import urllib.request
from pathlib import Path

os.environ["DAFT_PROGRESS_BAR"] = "0"

import daft
from daft.datatype import DataType
from daft.functions import video_file

HERE = Path(__file__).parent
CHARTS = HERE / "charts"
SHARD = HERE / "shard.mp4"
SHARD_URL = (
    "https://huggingface.co/datasets/pepijn223/egodex-test/resolve/main/"
    "videos/observation.image/chunk-000/file-000.mp4"
)
SHARDS = 8
TARGETS = [0.0, 5.0, 10.0, 15.0, 20.0]  # span the full shard -> decode all frames
WORKERS = [1, 2, 4, 8]


@daft.func.batch(return_dtype=DataType.float64(), use_process=True)
def _decode_mean(files, tss):
    """Batched decode grouped by shard; returns a scalar per row to isolate decode cost."""
    import av

    file_list = files.to_pylist()
    ts_list = tss.to_pylist()
    by_shard: dict = {}
    shard_file: dict = {}
    for i, f in enumerate(file_list):
        by_shard.setdefault(f.path, []).append((i, float(ts_list[i])))
        shard_file[f.path] = f
    results = [0.0] * len(file_list)
    for path, targets in by_shard.items():
        earliest = min(t for _, t in targets)
        latest = max(t for _, t in targets)
        best = {row: (1e9, 0.0) for row, _ in targets}
        with shard_file[path].open() as f_open, av.open(f_open) as container:
            stream = container.streams.video[0]
            container.seek(max(0, int(earliest * av.time_base)), backward=True)
            for vf in container.decode(stream):
                if vf.pts is None:
                    continue
                cts = float(vf.pts * stream.time_base)
                mean = float(vf.to_ndarray(format="rgb24").mean())
                for row, target in targets:
                    dist = abs(cts - target)
                    if dist < best[row][0]:
                        best[row] = (dist, mean)
                if cts >= latest + 0.1:
                    break
        for row, _ in targets:
            results[row] = best[row][1]
    return results


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


def run(workers, shards):
    urls, tss = [], []
    for s in shards:
        for t in TARGETS:
            urls.append(s)
            tss.append(t)
    df = daft.from_pydict({"url": urls, "ts": tss})
    df = df.with_column("v", video_file(df["url"])).into_batches(len(TARGETS))
    fn = _decode_mean.with_concurrency(workers)
    df = df.with_column("m", fn(df["v"], df["ts"]))
    start = time.perf_counter()
    df.select("m").collect()
    return time.perf_counter() - start


def main():
    shards = ensure_shards(SHARDS)
    print(f"{SHARDS} shards, full-span decode (~632 frames each)\n")
    results = []
    base = None
    for w in WORKERS:
        dt = run(w, shards)
        base = base or dt
        results.append((w, dt))
        print(f"  workers={w:2d}  wall={dt:6.2f}s  speedup={base / dt:4.1f}x", flush=True)

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot([w for w, _ in results], [d for _, d in results], "o-", color="#2ca02c")
    ax.set_title(f"Batched decode: wall time vs worker processes\n({SHARDS} shards, full-span decode)")
    ax.set_xlabel("worker processes")
    ax.set_ylabel("wall time (s)")
    ax.set_xticks([w for w, _ in results])
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    CHARTS.mkdir(exist_ok=True)
    fig.savefig(CHARTS / "chart_workers.png", dpi=130)
    print("\nwrote charts/chart_workers.png")


if __name__ == "__main__":
    main()
