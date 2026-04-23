"""Run an asof-join benchmark on a Ray cluster with Daft.

Usage (from the head node after `ray up deployment.yaml`):
  python run_benchmark.py --scale small --n_runs 3 --workers 4
"""

from __future__ import annotations

import argparse
import time

import daft
import ray


S3_BASE = "s3://eventual-dev-benchmarking-fixtures/asof-join"


def run_asof_join(left_path: str, right_path: str, n_partitions: int) -> int:
    left = daft.read_parquet(left_path).repartition(n_partitions)
    right = daft.read_parquet(right_path).repartition(n_partitions)
    result = left.join_asof(right, on="ts", by="entity", suffix="_right").collect()
    return len(result)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", default="small", choices=["small", "medium", "large"])
    parser.add_argument("--n_runs", type=int, default=3)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()

    left_path = f"{S3_BASE}/{args.scale}/left/"
    right_path = f"{S3_BASE}/{args.scale}/right/"

    ray.init(address="auto")
    daft.set_runner_ray()

    # Wait for workers to join the cluster.
    expected_workers = args.workers
    print(f"Waiting for {expected_workers} workers...", flush=True)
    while True:
        nodes = [n for n in ray.nodes() if n["Alive"] and not n.get("IsHeadNode", False)]
        if len(nodes) >= expected_workers:
            break
        time.sleep(5)
    print(f"{len(nodes)} workers ready.")

    print(f"Ray cluster: {ray.cluster_resources()}")
    print(f"Scale: {args.scale} | Runs: {args.n_runs} | Workers: {args.workers}")
    print(f"Left:  {left_path}")
    print(f"Right: {right_path}")
    print()

    # Warmup
    print("Warmup run...", flush=True)
    run_asof_join(left_path, right_path, args.workers)
    print("Warmup done.\n")

    times = []
    for i in range(1, args.n_runs + 1):
        start = time.perf_counter()
        nrows = run_asof_join(left_path, right_path, args.workers)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        print(f"Run {i}/{args.n_runs}: {elapsed:.3f}s ({nrows} rows)")

    times.sort()
    median = times[len(times) // 2]
    print(f"\nMedian wall time: {median:.3f}s")


if __name__ == "__main__":
    main()
