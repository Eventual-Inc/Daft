"""Out-of-core shuffle benchmark: Daft (mr / psm / flt) vs Ray Data on Ray cluster.

Reads TPC-H lineitem from eventual-dev S3 fixtures and shuffles by L_ORDERKEY.

Output: /opt/daft-out/<engine>_<algo>_<size>gb_<parts>p (per-worker partitions).
Spill (flight_shuffle only): /opt/daft-spill (EBS-backed).

Usage:
    python benchmark_shuffle.py --engine daft --algo flight_shuffle --size-gb 50 --partitions 100
    python benchmark_shuffle.py --engine ray_data --size-gb 50 --partitions 100
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import shutil
import socket
import sys
import time
import traceback
from datetime import datetime

# Pick sf based on requested size; sf100=22GB, sf1000=227GB, sf10000=2.2TB.
SF_FIXTURES = {
    100:   ("s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/100_0/32/parquet/lineitem/",   "L_ORDERKEY"),
    1000:  ("s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/1000_0/512/parquet/lineitem/", "L_ORDERKEY"),
    10000: ("s3://daft-public-datasets/tpch-lineitem/10000_0/10000/parquet/lineitem/",                      "l_orderkey"),
}
APPROX_BYTES_PER_ROW = 145  # TPC-H lineitem in-memory Arrow

OUTPUT_BASE = "/opt/daft-out"
SPILL_DIRS  = ["/opt/daft-spill"]   # EBS-backed; no NVMe on this cluster


def pick_sf(size_gb: int) -> int:
    if size_gb <= 22:   return 100
    if size_gb <= 227:  return 1000
    return 10000


def list_s3_files(prefix: str) -> list[str]:
    import pyarrow.fs as fs
    assert prefix.startswith("s3://")
    bucket_key = prefix[5:]
    bucket, _, p = bucket_key.partition("/")
    f = fs.S3FileSystem(region="us-west-2", anonymous=False)
    sel = f"{bucket}/{p}" if p else f"{bucket}/"
    infos = f.get_file_info(fs.FileSelector(sel))
    return sorted("s3://" + i.path for i in infos if i.is_file)


def clear_output_dir(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def cluster_wide_rmtree(path: str) -> None:
    """Remove `path` on every Ray node (head + workers) so per-cell output
    doesn't accumulate on EBS across runs."""
    import ray
    node_ids = [n["NodeID"] for n in ray.nodes() if n.get("Alive")]
    @ray.remote(num_cpus=0)
    def _rm(p):
        import shutil, socket
        shutil.rmtree(p, ignore_errors=True)
        return socket.gethostname()
    refs = [
        _rm.options(
            scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(nid, soft=False)
        ).remote(path)
        for nid in node_ids
    ]
    ray.get(refs)


def run_daft(s3_paths: list[str], target_rows: int, num_partitions: int, key: str, algo: str, out_dir: str) -> dict:
    import daft

    daft.set_runner_ray(address="auto", noop_if_initialized=True)
    cfg = {"shuffle_algorithm": algo}
    if algo == "flight_shuffle":
        cfg["flight_shuffle_dirs"] = SPILL_DIRS
    daft.set_execution_config(**cfg)
    print(f"[daft] runner=ray  algo={algo}  spill={cfg.get('flight_shuffle_dirs')}", flush=True)

    df = daft.read_parquet(s3_paths).limit(target_rows)
    df = df.repartition(num_partitions, key)

    t0 = time.perf_counter()
    write_result = df.write_parquet(out_dir)
    elapsed = time.perf_counter() - t0

    rows = write_result.to_pydict().get("rows", [0])
    rows = sum(rows) if isinstance(rows, list) else rows
    return {"elapsed_s": elapsed, "rows": rows, "daft_version": daft.__version__}


def run_ray_data(s3_paths: list[str], target_rows: int, num_partitions: int, key: str, out_dir: str, mode: str = "materialize") -> dict:
    import ray
    import ray.data as rd

    ds = rd.read_parquet(s3_paths).limit(target_rows)
    ds = ds.repartition(num_partitions, keys=[key])

    t0 = time.perf_counter()
    if mode == "materialize":
        mat = ds.materialize()
        rows = mat.count()
    else:
        ds.write_parquet(out_dir)
        rows = target_rows
    elapsed = time.perf_counter() - t0
    return {"elapsed_s": elapsed, "rows": rows, "ray_version": ray.__version__, "ray_data_mode": mode}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, choices=["daft", "ray_data"])
    ap.add_argument("--algo", default=None,
                    choices=["map_reduce", "pre_shuffle_merge", "flight_shuffle"],
                    help="Required for --engine daft")
    ap.add_argument("--size-gb", type=int, required=True)
    ap.add_argument("--partitions", type=int, required=True)
    ap.add_argument("--results-file", default="/home/ec2-user/benchmark_results.jsonl")
    args = ap.parse_args()

    if args.engine == "daft" and not args.algo:
        ap.error("--algo required when --engine daft")

    sf = pick_sf(args.size_gb)
    s3_prefix, key = SF_FIXTURES[sf]
    target_rows = int(args.size_gb * 1024**3 / APPROX_BYTES_PER_ROW)

    cell_id = f"{args.engine}_{args.algo or 'na'}_{args.size_gb}gb_{args.partitions}p"
    out_dir = f"{OUTPUT_BASE}/{cell_id}"

    print(f"\n=== cell start: {cell_id}  sf={sf}  target_rows={target_rows:,} ===", flush=True)

    # Init Ray + report cluster size
    import ray
    ray.init(address="auto", logging_level="ERROR", ignore_reinit_error=True)
    cluster = ray.cluster_resources()
    print(f"[ray] cpus={cluster.get('CPU', 0)}  mem_gb={cluster.get('memory', 0)/1e9:.0f}  "
          f"objstore_gb={cluster.get('object_store_memory', 0)/1e9:.0f}", flush=True)

    # Verify worker daft version (avoid the CWD-source-tree trap)
    if args.engine == "daft":
        @ray.remote
        def probe():
            import daft
            return daft.__version__
        vers = ray.get([probe.options(scheduling_strategy="SPREAD").remote() for _ in range(32)])
        print(f"[probe] worker daft versions: {sorted(set(vers))}", flush=True)

    s3_paths = list_s3_files(s3_prefix)
    print(f"[input] sf={sf}  total_files={len(s3_paths)}  using all (limited by .limit({target_rows:,}))", flush=True)
    clear_output_dir(out_dir)
    cluster_wide_rmtree(out_dir)

    result = {
        "ts": datetime.now().isoformat(),
        "host": socket.gethostname(),
        "engine": args.engine,
        "algo": args.algo,
        "size_gb": args.size_gb,
        "partitions": args.partitions,
        "sf_used": sf,
        "target_rows": target_rows,
        "cluster_cpus": cluster.get("CPU", 0),
    }

    try:
        if args.engine == "daft":
            info = run_daft(s3_paths, target_rows, args.partitions, key, args.algo, out_dir)
        else:
            info = run_ray_data(s3_paths, target_rows, args.partitions, key, out_dir)
        result.update(info)
        result["status"] = "ok"
        result["throughput_gbps"] = round(args.size_gb / info["elapsed_s"], 3) if info["elapsed_s"] > 0 else None
        print(f"[result] {cell_id} -> {info['elapsed_s']:.1f}s  ({result['throughput_gbps']} GB/s)", flush=True)
    except Exception as e:
        traceback.print_exc()
        result["status"] = "error"
        result["error"] = f"{type(e).__name__}: {e}"

    with open(args.results_file, "a") as f:
        f.write(json.dumps(result) + "\n")

    # Cluster-wide cleanup so EBS doesn't fill across cells
    try:
        cluster_wide_rmtree(out_dir)
    except Exception as e:
        print(f"[cleanup] warning: {e}", flush=True)
    gc.collect()
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
