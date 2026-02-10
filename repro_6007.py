"""Reproduction script for issue #6007: overflow when adding duration to instant.

Exercises the same code path as the reporter:
  DataFrame → PartitionedDeltalake Write with 2840 partitions

Run with:
  RUST_BACKTRACE=1 DAFT_PROGRESS_BAR=0 DAFT_RUNNER=native python repro_6007.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import time

import daft

# Match reporter's partition count
NUM_PARTITIONS = 2840

# Start with 10K rows/partition (28.4M total). Increase if needed.
ROWS_PER_PARTITION = 10_000
TOTAL_ROWS = NUM_PARTITIONS * ROWS_PER_PARTITION

print("=== Issue #6007 Reproduction ===")
print(f"Partitions: {NUM_PARTITIONS}")
print(f"Rows/partition: {ROWS_PER_PARTITION:,}")
print(f"Total rows: {TOTAL_ROWS:,}")
print("deltalake version: ", end="")
try:
    import deltalake

    print(deltalake.__version__)
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)

print()

# Generate data
print("Generating data...")
t0 = time.time()
df = daft.from_pydict(
    {
        "id": list(range(TOTAL_ROWS)),
        "value": [f"data_{i}" for i in range(TOTAL_ROWS)],
        "partition_key": [i % NUM_PARTITIONS for i in range(TOTAL_ROWS)],
    }
)
print(f"  Data generated in {time.time() - t0:.1f}s")

# Write to local Delta Lake
output_path = os.path.join(tempfile.mkdtemp(), "delta_table")
print(f"Writing to: {output_path}")
print(f"Starting write at {time.strftime('%H:%M:%S')}...")
t1 = time.time()

try:
    result = df.write_deltalake(output_path, partition_cols=["partition_key"])
    elapsed = time.time() - t1
    print(f"\nWrite completed in {elapsed:.1f}s ({elapsed / 60:.1f}m)")
    print(f"Result schema: {result.schema()}")
    print(f"Result rows: {result.count_rows()}")
    print("\n=== NO PANIC - Issue did NOT reproduce locally ===")
    print("This suggests the panic is cloud-storage specific (object_store retry logic).")
except Exception as e:
    elapsed = time.time() - t1
    print(f"\n=== EXCEPTION after {elapsed:.1f}s ===")
    print(f"Type: {type(e).__name__}")
    print(f"Message: {e}")
    print("Check stderr for panic hook backtrace above ^^^")
    raise
