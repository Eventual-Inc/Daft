"""Daft asof-join benchmark.

Runs a single join for a given scale, wrapped in a memray tracker.

Usage:
    python benchmark.py --scale small

Output:
    {"status": "ok", "wall_time_s": 1.23, "start_ts": 1.0, "end_ts": 2.23, "memray_output": "<path>"}
  | {"status": "error", "message": "<traceback>"}
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time
import traceback

import daft
import memray

from data_generation import SCALES

# Switch to the S3 root for Ray runs:
# DATA_ROOT = "s3://<your-bucket>/asof-join"
DATA_ROOT = str(pathlib.Path(__file__).parent.parent / "data" / "asof_join")

# For distributed runs, use Ray instead:
# daft.set_runner_ray()
daft.set_runner_native()


def _run(left_path: str, right_path: str) -> int:
    left = daft.read_parquet(left_path)
    right = daft.read_parquet(right_path)
    return len(left.join_asof(right, on="ts", by="entity", suffix="_right").collect())


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", choices=list(SCALES), required=True)
    args = parser.parse_args()

    left_path = f"{DATA_ROOT}/{args.scale}/left"
    right_path = f"{DATA_ROOT}/{args.scale}/right"
    memray_output = "asof_join_memray.bin"

    try:
        if os.path.exists(memray_output):
            os.remove(memray_output)
        tracker = memray.Tracker(memray_output, native_traces=True)
        tracker.__enter__()
    except Exception:
        print(json.dumps({"status": "error", "message": traceback.format_exc()}), flush=True)
        sys.exit(1)

    try:
        start_ts = time.time()
        start = time.perf_counter()
        _run(left_path, right_path)
        end = time.perf_counter()
        end_ts = time.time()
        tracker.__exit__(None, None, None)
        print(
            json.dumps(
                {
                    "status": "ok",
                    "wall_time_s": end - start,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "memray_output": memray_output,
                }
            ),
            flush=True,
        )
    except Exception:
        print(json.dumps({"status": "error", "message": traceback.format_exc()}), flush=True)


if __name__ == "__main__":
    main()
