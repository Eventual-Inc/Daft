"""Repartition benchmark: Ray Data vs Daft (multiple shuffle algorithms).

Run a single (engine, algo, scale, partitions) cell and append one JSON line
to /tmp/benchmark_results.jsonl.

Usage:
  python /tmp/benchmark.py \\
      --engine daft --algo map_reduce \\
      --scale sf100 --partitions 32 \\
      --output s3://colinho-test/dump --run-name pypi_daft \\
      [--limit-files N]
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import socket
import sys
import time
import traceback

# -- Static sources from the user's goal ---------------------------------------
DEFAULT_OUTPUT_PATH = "s3://colinho-test/dump"
LINEITEM_PATHS = {
    "sf100":   ("s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/100_0/32/parquet/lineitem/*",     "L_ORDERKEY"),
    "sf1000":  ("s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/1000_0/512/parquet/lineitem/*",   "L_ORDERKEY"),
    "sf10000": ("s3://daft-public-datasets/tpch-lineitem/10000_0/10000/parquet/lineitem/*",                        "l_orderkey"),
}
S3_PREFIX_BUCKET = "colinho-test"


def expand_files(glob_path: str, limit: int | None) -> list[str]:
    """Materialize to an explicit file list. Ray Data doesn't accept a
    bare '*' glob, so engines always get a list."""
    import pyarrow.fs as fs
    assert glob_path.startswith("s3://") and glob_path.endswith("/*")
    bucket_key = glob_path[len("s3://"):-2]
    bucket, _, prefix = bucket_key.partition("/")
    f = fs.S3FileSystem(region="us-west-2", anonymous=False)
    selector_path = f"{bucket}/{prefix}/" if prefix else f"{bucket}/"
    infos = f.get_file_info(fs.FileSelector(selector_path))
    files = sorted(i.path for i in infos if i.is_file)
    if limit is not None:
        files = files[:limit]
    return ["s3://" + p for p in files]


def clean_output_prefix(output_path: str, key: str) -> None:
    """Delete the per-run S3 prefix to avoid mixing past results."""
    import pyarrow.fs as fs
    assert output_path.startswith("s3://")
    bucket_key = output_path[len("s3://"):]
    bucket, _, prefix = bucket_key.partition("/")
    full_prefix = f"{prefix}/{key}" if prefix else key
    f = fs.S3FileSystem(region="us-west-2")
    try:
        infos = f.get_file_info(fs.FileSelector(f"{bucket}/{full_prefix}/", recursive=True))
        for info in infos:
            if info.is_file:
                f.delete_file(info.path)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[clean] warning: {e}")


def print_versions(engine: str) -> dict:
    info = {
        "host": socket.gethostname(),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
    }
    import ray
    info["ray"] = ray.__version__
    if engine == "daft":
        import daft
        info["daft"] = daft.__version__
    print(f"[versions] {info}")
    return info


def run_daft(s3_paths: list[str], num_partitions: int, output_path: str, key_col: str, algo: str) -> tuple[float, str]:
    import daft

    daft.set_runner_ray(address="auto")
    cfg_kwargs = {"shuffle_algorithm": algo}
    if algo == "flight_shuffle":
        cfg_kwargs["flight_shuffle_dirs"] = ["/tmp/ray/spill", "/mnt2/spill"]
    daft.set_execution_config(**cfg_kwargs)
    print(f"[daft] runner=ray  shuffle_algorithm={algo}  flight_dirs={cfg_kwargs.get('flight_shuffle_dirs')}")

    df = daft.read_parquet(s3_paths)
    df = df.repartition(num_partitions, key_col)

    print(f"[daft] explain:")
    try:
        df.explain(True)
    except Exception as e:
        print(f"[daft] explain failed: {e}")

    t0 = time.time()
    df.write_parquet(output_path)
    elapsed = time.time() - t0
    return elapsed, daft.__version__


def run_ray_data(s3_paths: list[str], num_partitions: int, output_path: str, key_col: str) -> tuple[float, str]:
    import ray
    import ray.data as ray_data

    ds = ray_data.read_parquet(s3_paths)
    ds = ds.repartition(num_partitions, keys=[key_col])

    print(f"[ray_data] explain:")
    try:
        ds.explain()
    except Exception as e:
        print(f"[ray_data] explain failed: {e}")

    t0 = time.time()
    ds.write_parquet(output_path)
    elapsed = time.time() - t0
    return elapsed, ray.__version__


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, choices=["daft", "ray_data"])
    ap.add_argument("--algo", default=None,
                    help="daft shuffle_algorithm: auto|map_reduce|pre_shuffle_merge|flight_shuffle (required for --engine daft)")
    ap.add_argument("--scale", required=True, choices=list(LINEITEM_PATHS))
    ap.add_argument("--limit-files", type=int, default=None,
                    help="Restrict to first N files of the scale (for finer-grained data sizes)")
    ap.add_argument("--partitions", type=int, required=True)
    ap.add_argument("--output-bucket", default=DEFAULT_OUTPUT_PATH)
    ap.add_argument("--run-name", default="run",
                    help="Identifier — also used as S3 sub-prefix so concurrent runs don't collide")
    ap.add_argument("--results-file", default="/tmp/benchmark_results.jsonl")
    args = ap.parse_args()

    if args.engine == "daft" and not args.algo:
        ap.error("--algo required when --engine daft")

    src_glob, key_col = LINEITEM_PATHS[args.scale]
    s3_paths = expand_files(src_glob, args.limit_files)
    scale_label = f"{args.scale}" + (f"_top{args.limit_files}" if args.limit_files else "")
    output_key = f"{args.run_name}/{args.engine}/{args.algo or 'na'}/{scale_label}/p{args.partitions}"
    output_path = f"{args.output_bucket.rstrip('/')}/{output_key}"

    print(f"\n=== run start: {args.run_name} engine={args.engine} algo={args.algo} scale={scale_label} partitions={args.partitions} ===")
    print(f"[config] src files: {len(s3_paths)}  key={key_col}")
    print(f"[config] output: {output_path}")
    print_versions(args.engine)

    print("[clean] removing prior output prefix...")
    clean_output_prefix(args.output_bucket, output_key)

    import ray
    try:
        daft_trace = os.environ.get("DAFT_TRACE")
        runtime_env = {"env_vars": {"DAFT_TRACE": daft_trace}} if daft_trace else None
        ray.init(address="auto", logging_level="ERROR", ignore_reinit_error=True, runtime_env=runtime_env)
        n_alive = sum(1 for n in ray.nodes() if n.get("Alive"))
        total_cpus = ray.cluster_resources().get("CPU", 0)
        print(f"[ray] nodes_alive={n_alive}  cluster_cpus={total_cpus}")
    except Exception as e:
        print(f"[ray] init warning: {e}")

    result = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "run_name": args.run_name,
        "engine": args.engine,
        "algo": args.algo,
        "scale": scale_label,
        "limit_files": args.limit_files,
        "partitions": args.partitions,
        "src": src_glob,
        "key_col": key_col,
        "output": output_path,
    }

    try:
        if args.engine == "daft":
            elapsed, ver = run_daft(s3_paths, args.partitions, output_path, key_col, args.algo)
            result["daft_version"] = ver
        else:
            elapsed, ver = run_ray_data(s3_paths, args.partitions, output_path, key_col)
            result["ray_version"] = ver
        result["status"] = "ok"
        result["seconds"] = round(elapsed, 3)
        print(f"[result] {args.engine} {args.algo or ''} {scale_label} p={args.partitions}  -> {elapsed:.2f}s")
    except Exception as e:
        traceback.print_exc()
        result["status"] = "error"
        result["error"] = f"{type(e).__name__}: {e}"
        result["seconds"] = None

    with open(args.results_file, "a") as f:
        f.write(json.dumps(result) + "\n")

    # Hard-exit; ray.init in this process holds resources.
    gc.collect()
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    sys.exit(main())
