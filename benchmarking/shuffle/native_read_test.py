"""Read 8 files from sf10000 with daft native runner. Time wall + count rows.

Argv:
  --select-all     -> read full schema
  --select-key     -> only l_orderkey column (smaller decode work)
"""
import argparse
import os
import socket
import sys
import time

import daft

ap = argparse.ArgumentParser()
ap.add_argument("--repeat", type=int, default=3, help="iterations")
ap.add_argument("--select", choices=["all", "key", "key_extprice"], default="all")
args = ap.parse_args()

print(f"daft={daft.__version__}  host={socket.gethostname()}")
daft.set_runner_native()

import pyarrow.fs as fs
PREFIX = "daft-public-datasets/tpch-lineitem/10000_0/10000/parquet/lineitem/"
f = fs.S3FileSystem(region="us-west-2")
infos = f.get_file_info(fs.FileSelector(PREFIX))
files = sorted(i.path for i in infos if i.is_file)[:8]
paths = ["s3://" + p for p in files]
print(f"files: {len(paths)}  first={paths[0]}")

def run_once():
    df = daft.read_parquet(paths)
    if args.select == "key":
        df = df.select("l_orderkey")
    elif args.select == "key_extprice":
        df = df.select("l_orderkey", "l_extendedprice")
    t0 = time.time()
    # Force a full materialized read — actually decode every row group
    table = df.to_arrow()
    elapsed = time.time() - t0
    return elapsed, table.num_rows, sum(b.nbytes for b in table.column(0).chunks)

for i in range(args.repeat):
    elapsed, n, b0 = run_once()
    print(f"  iter {i+1}: {elapsed:.3f}s  rows={n}  col0_bytes={b0}  select={args.select}")
