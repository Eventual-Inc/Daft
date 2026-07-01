"""Sparse-timestamps case, remote shard - the batched decode's worst case.

N timestamps spread across the whole shard. Batched opens once and decodes the span;
original opens once per frame. Shows that batched still wins remotely even when the
frames are sparse, because one saved download outweighs decoding the extra frames.
Uses raw PyAV (the decode strategies in isolation), no Daft.

    python sparse.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import av

HERE = Path(__file__).parent
CHARTS = HERE / "charts"
URL = (
    "https://huggingface.co/datasets/pepijn223/egodex-test/resolve/main/"
    "videos/observation.image/chunk-000/file-000.mp4"
)
TAIL = 0.1
NS = [2, 4, 8, 16]
SPAN = 20.0  # spread targets across the full ~21s shard


def span_decode(targets):
    """Batched strategy: one open, decode earliest..latest in a single pass."""
    lo, hi = min(targets), max(targets)
    with av.open(URL) as c:
        st = c.streams.video[0]
        c.seek(max(0, int(lo * av.time_base)), backward=True)
        for vf in c.decode(st):
            if vf.pts is None:
                continue
            cts = float(vf.pts * st.time_base)
            vf.to_ndarray(format="rgb24")
            if cts >= hi + TAIL:
                break


def original(targets):
    """Original strategy: reopen the shard once per frame."""
    for tg in targets:
        with av.open(URL) as c:
            st = c.streams.video[0]
            c.seek(max(0, int(tg * av.time_base)), backward=True)
            for vf in c.decode(st):
                if vf.pts is None:
                    continue
                cts = float(vf.pts * st.time_base)
                vf.to_ndarray(format="rgb24")
                if cts >= tg + TAIL:
                    break


def main():
    ns, span, orig = [], [], []
    for n in NS:
        targets = [SPAN * i / (n - 1) for i in range(n)]
        t = time.perf_counter()
        span_decode(targets)
        sp = time.perf_counter() - t
        t = time.perf_counter()
        original(targets)
        og = time.perf_counter() - t
        ns.append(n)
        span.append(sp)
        orig.append(og)
        print(f"n={n:2d}  span(batched)={sp:5.2f}s  original={og:5.2f}s", flush=True)

    (HERE / "sparse.json").write_text(json.dumps({"n": ns, "span": span, "orig": orig}))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(ns, orig, "o-", color="#d62728", label="original (one open per frame)")
    ax.plot(ns, span, "s-", color="#2ca02c", label="batched (one open, decode span)")
    ax.set_title("Sparse frames, remote shard: wall time vs frame count\n(frames spread across the full ~21s shard)")
    ax.set_xlabel("frames requested (spread across shard)")
    ax.set_ylabel("wall time (s)")
    ax.set_xticks(ns)
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    CHARTS.mkdir(exist_ok=True)
    fig.savefig(CHARTS / "chart_sparse.png", dpi=130)
    print("wrote charts/chart_sparse.png")


if __name__ == "__main__":
    main()
