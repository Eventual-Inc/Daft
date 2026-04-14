"""Long-lived worker subprocess for a single benchmark system.

The runner spawns one instance of this script per system. The library
(pandas / polars / daft) is imported once at startup, then the worker loops
over lines on stdin. Each line is a JSON command; the worker executes the join
and prints a JSON result line back to stdout.

Protocol:
  stdin  line: {"left": "<path>", "right": "<path>"}
  stdout line: {"status": "ok", "wall_time_s": 1.23, "start_ts": 1.0, "end_ts": 2.23}
             | {"status": "error", "message": "<traceback>"}
  stdin  line: "exit" -> worker exits cleanly

Usage (managed by __main__.py, not invoked directly):
  python worker.py <system>   where system in {pandas, polars, daft_native}
"""

from __future__ import annotations

import json
import sys
import time
import traceback


def _init(system: str):
    """Import the library and return a callable run(left, right) -> int."""
    if system == "pandas":
        import pandas as pd

        def run(left_path: str, right_path: str) -> int:
            left = pd.read_parquet(left_path).sort_values("ts")
            right = pd.read_parquet(right_path).sort_values("ts")
            result = pd.merge_asof(left, right, on="ts", by="entity", suffixes=("", "_right"))
            return len(result)

        return run

    if system == "polars":
        import polars as pl

        def run(left_path: str, right_path: str) -> int:
            left = pl.read_parquet(left_path).sort("ts")
            right = pl.read_parquet(right_path).sort("ts")
            result = left.join_asof(right, on="ts", by="entity", suffix="_right")
            return len(result)

        return run

    if system == "daft_native":
        import daft

        daft.set_runner_native()

        def run(left_path: str, right_path: str) -> int:
            left = daft.read_parquet(left_path)
            right = daft.read_parquet(right_path)
            result = left.join_asof(right, on="ts", by="entity", suffix="_right").collect()
            return len(result)

        return run

    raise ValueError(f"Unknown system: {system!r}")


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python worker.py <system>", file=sys.stderr)
        sys.exit(1)

    system = sys.argv[1]

    try:
        run = _init(system)
    except Exception:
        print(json.dumps({"status": "error", "message": traceback.format_exc()}), flush=True)
        sys.exit(1)

    # Signal to the parent that init is complete.
    print("ready", flush=True)

    for line in sys.stdin:
        line = line.strip()
        if line == "exit":
            break
        try:
            cmd = json.loads(line)
            start_ts = time.time()
            start = time.perf_counter()
            run(cmd["left"], cmd["right"])
            end = time.perf_counter()
            end_ts = time.time()
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "wall_time_s": end - start,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                    }
                ),
                flush=True,
            )
        except Exception:
            print(json.dumps({"status": "error", "message": traceback.format_exc()}), flush=True)


if __name__ == "__main__":
    main()
