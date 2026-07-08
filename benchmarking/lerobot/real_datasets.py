"""Decode N frames from a real HF LeRobot dataset and dump per-frame hashes + timing.

Run once per reader revision (like sweep.py), then compare the outputs:

    python real_datasets.py <dataset_id> <out.json> [n_rows=16]
    python real_datasets.py --compare orig.json batched.json

Rows are pinned to episode 0's first N frames so both runs decode identical rows.
Wall time covers the full ``lerobot.read(..., load_video_frames=True)`` pipeline.
"""

from __future__ import annotations

import hashlib
import json
import sys
import time


def compare(path_a: str, path_b: str) -> int:
    a, b = json.load(open(path_a)), json.load(open(path_b))
    if a == b:
        print("IDENTICAL (pixels)")
        return 0
    mism = sum(1 for ra, rb in zip(a["rows"], b["rows"]) if ra != rb)
    print(f"MISMATCH: {mism} differing rows (of {len(a['rows'])} vs {len(b['rows'])})")
    return 1


def main() -> int:
    if sys.argv[1] == "--compare":
        return compare(sys.argv[2], sys.argv[3])

    import numpy as np

    import daft
    from daft.datasets import lerobot

    ds, out_path = sys.argv[1], sys.argv[2]
    n_rows = int(sys.argv[3]) if len(sys.argv) > 3 else 16

    t0 = time.perf_counter()
    df = lerobot.read(ds, load_video_frames=True)
    df = df.where((daft.col("episode_index") == 0) & (daft.col("frame_index") < n_rows))
    data = df.collect().to_pydict()
    wall = time.perf_counter() - t0

    def is_image(v) -> bool:
        if hasattr(v, "mode"):  # PIL
            return True
        arr = np.asarray(v) if v is not None else None
        return arr is not None and arr.ndim == 3 and arr.shape[-1] in (1, 3, 4)

    img_cols = [c for c, vals in data.items() if len(vals) > 0 and is_image(vals[0])]
    assert img_cols, f"no image columns detected in {sorted(data.keys())}"

    rows = []
    for i in range(len(data["episode_index"])):
        row = {"episode_index": int(data["episode_index"][i]), "frame_index": int(data["frame_index"][i])}
        for c in sorted(img_cols):
            arr = np.asarray(data[c][i])
            row[c] = {"shape": list(arr.shape), "sha256": hashlib.sha256(arr.tobytes()).hexdigest()}
        rows.append(row)
    rows.sort(key=lambda r: (r["episode_index"], r["frame_index"]))
    assert rows, "no rows decoded"

    with open(out_path, "w") as f:
        json.dump({"rows": rows, "img_cols": sorted(img_cols)}, f, indent=1, sort_keys=True)
    print(f"OK {ds}: {len(rows)} rows x {len(img_cols)} cams in {wall:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
