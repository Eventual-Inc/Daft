"""Generate asof-join benchmark datasets and write them to local disk.

Usage (one-time offline step):
    python -m benchmarking.asof_join.data_generation --scale small
    python -m benchmarking.asof_join.data_generation --scale medium
    python -m benchmarking.asof_join.data_generation --all

Files written locally:
    benchmarking/data/asof_join/<scale>/left/part-00000.parquet
    ...
    benchmarking/data/asof_join/<scale>/right/part-00000.parquet
    ...

Design choices:
- Clustered timestamps: timestamps are generated around 1,000 cluster centers with
  jitter, mimicking real-world bursty time-series data.
- Skewed entity distribution (SKEW = 1.0 Zipf exponent): some entities are much more
  common than others, stressing join implementations that don't handle skew well.
- Fixed seed (SEED = 42): results are reproducible across partitions.
"""

from __future__ import annotations

import argparse
import pathlib

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

LOCAL_DATA_ROOT = pathlib.Path(__file__).parent.parent / "data" / "asof_join"

SCALES: dict[str, dict[str, int]] = {
    "small": {"right_rows": 10_000_000, "left_rows": 1_000_000},
    "medium": {"right_rows": 100_000_000, "left_rows": 10_000_000},
    "large": {"right_rows": 500_000_000, "left_rows": 50_000_000},
}

N_ENTITIES = 10_000
SEED = 42
TS_MAX = 10**12
N_CLUSTERS = 1_000
CLUSTER_WIDTH = 10**6  # jitter ± around each cluster centre
SKEW = 1.0  # Zipf exponent for entity frequency
ROWS_PER_FILE = 5_000_000  # ~256 MB per file

ENTITY_LABELS = np.array([f"e{i:05d}" for i in range(N_ENTITIES)], dtype=object)


def _entity_weights() -> np.ndarray:
    ranks = np.arange(1, N_ENTITIES + 1, dtype=np.float64)
    weights = 1.0 / ranks**SKEW
    return weights / weights.sum()


def _generate_table(n_rows: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)

    centers = rng.integers(0, TS_MAX, size=N_CLUSTERS, dtype=np.int64)
    assignments = rng.integers(0, N_CLUSTERS, size=n_rows, dtype=np.int32)
    jitter = rng.integers(-CLUSTER_WIDTH, CLUSTER_WIDTH, size=n_rows, dtype=np.int64)

    ts = np.clip(centers[assignments] + jitter, 0, TS_MAX - 1)

    entity_idx = rng.choice(N_ENTITIES, size=n_rows, p=_entity_weights())
    entity = ENTITY_LABELS[entity_idx]

    vals = rng.random(n_rows)

    return pa.table(
        {
            "ts": pa.array(ts, type=pa.int64()),
            "entity": pa.array(entity, type=pa.utf8()),
            "val": pa.array(vals, type=pa.float64()),
        }
    )


def _n_files(n_rows: int) -> int:
    return max(1, round(n_rows / ROWS_PER_FILE))


def _write_table_to_disk(table: pa.Table, local_path: pathlib.Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, local_path, compression="snappy")
    size_gb = local_path.stat().st_size / 1e9
    print(f"      wrote {local_path}  ({size_gb:.2f} GB)")


def generate_scale(scale: str) -> None:
    """Generate left and right tables for a given scale, partitioned into multiple files."""
    cfg = SCALES[scale]

    for side, n_rows, seed_offset in [
        ("right", cfg["right_rows"], 0),
        ("left", cfg["left_rows"], 1),
    ]:
        n_files = _n_files(n_rows)
        base_rows = n_rows // n_files
        print(f"  generating {side} ({n_rows:,} rows → {n_files} files) ...", flush=True)

        for i in range(n_files):
            chunk_rows = base_rows if i < n_files - 1 else n_rows - base_rows * (n_files - 1)
            table = _generate_table(n_rows=chunk_rows, seed=SEED + seed_offset + i)
            filename = f"part-{i:05d}.parquet"
            local_path = LOCAL_DATA_ROOT / scale / side / filename
            _write_table_to_disk(table, local_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate asof-join benchmark datasets.")
    scale_group = parser.add_mutually_exclusive_group(required=True)
    scale_group.add_argument("--scale", choices=list(SCALES), help="Generate a single scale.")
    scale_group.add_argument("--all", action="store_true", help="Generate all scales.")
    args = parser.parse_args()

    scales = list(SCALES) if args.all else [args.scale]

    for scale in scales:
        print(f"\n=== {scale} ===")
        generate_scale(scale)

    print("\nDone.")


if __name__ == "__main__":
    main()
