"""Generate asof-join benchmark datasets and write them to S3.

Usage (one-time offline step):
    python -m benchmarking.asof_join.data_generation --scale small
    python -m benchmarking.asof_join.data_generation --scale medium
    python -m benchmarking.asof_join.data_generation --all

    # Write to a local directory instead of S3:
    python -m benchmarking.asof_join.data_generation --scale small --local_output_dir /tmp/asof_data
    python -m benchmarking.asof_join.data_generation --all --local_output_dir /tmp/asof_data

Files written to S3:
    s3://eventual-dev-benchmarking-fixtures/asof-join/<scale>/left.parquet
    s3://eventual-dev-benchmarking-fixtures/asof-join/<scale>/right.parquet

Design choices:
- Clustered timestamps: timestamps are generated around 1,000 cluster centers with
  jitter, mimicking real-world bursty time-series data.
- Skewed entity distribution (SKEW = 1.0 Zipf exponent): some entities are much more
  common than others, stressing join implementations that don't handle skew well.
- Fixed seed (SEED = 42): results are reproducible.
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

S3_BUCKET = "eventual-dev-benchmarking-fixtures"
S3_PREFIX = "asof-join"

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

ENTITY_LABELS = np.array([f"e{i:05d}" for i in range(N_ENTITIES)], dtype=object)

S3_PATHS: dict[str, dict[str, str]] = {
    scale: {
        "left": f"s3://{S3_BUCKET}/{S3_PREFIX}/{scale}/left.parquet",
        "right": f"s3://{S3_BUCKET}/{S3_PREFIX}/{scale}/right.parquet",
    }
    for scale in SCALES
}


def _entity_weights() -> np.ndarray:
    ranks = np.arange(1, N_ENTITIES + 1, dtype=np.float64)
    weights = 1.0 / ranks**SKEW
    return weights / weights.sum()


def _generate_table(n_rows: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)

    # Pick N_CLUSTERS anchor timestamps uniformly across the time range, then
    # assign each row to one cluster and apply random jitter so timestamps
    # cluster around the anchors rather than being spread uniformly.
    centers = rng.integers(0, TS_MAX, size=N_CLUSTERS, dtype=np.int64)
    assignments = rng.integers(0, N_CLUSTERS, size=n_rows, dtype=np.int32)
    jitter = rng.integers(-CLUSTER_WIDTH, CLUSTER_WIDTH, size=n_rows, dtype=np.int64)

    ts = np.clip(centers[assignments] + jitter, 0, TS_MAX - 1)

    # Sample entities with Zipf-skewed probabilities so high-frequency entities
    # dominate, stressing skew handling in the join.
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


def _upload_table_to_s3(table: pa.Table, s3_path: str) -> None:
    """Write a PyArrow table to an S3 path as a Snappy-compressed Parquet file."""
    assert s3_path.startswith("s3://"), f"Expected s3:// path, got: {s3_path}"
    path_without_prefix = s3_path[len("s3://") :]
    bucket, _, key = path_without_prefix.partition("/")

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3 = boto3.client("s3")
    s3.upload_fileobj(buf, bucket, key)
    print(f"    uploaded to {s3_path}  ({buf.tell() / 1e9:.2f} GB)")


def generate_scale(scale: str, output_dir: Path | None = None) -> None:
    """Generate left and right tables for a given scale."""
    cfg = SCALES[scale]

    for side, n_rows, seed_offset in [
        ("right", cfg["right_rows"], 0),
        ("left", cfg["left_rows"], 1),
    ]:
        print(f"  generating {side} ({n_rows:,} rows) ...", flush=True)
        table = _generate_table(n_rows=n_rows, seed=SEED + seed_offset)

        if output_dir is not None:
            scale_dir = output_dir / scale
            scale_dir.mkdir(parents=True, exist_ok=True)
            out_path = scale_dir / f"{side}.parquet"
            pq.write_table(table, out_path, compression="snappy")
            print(f"    wrote {out_path}  ({out_path.stat().st_size / 1e9:.2f} GB on disk)")
        else:
            s3_path = S3_PATHS[scale][side]
            _upload_table_to_s3(table, s3_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate asof-join benchmark datasets.")
    scale_group = parser.add_mutually_exclusive_group(required=True)
    scale_group.add_argument("--scale", choices=list(SCALES), help="Generate a single scale.")
    scale_group.add_argument("--all", action="store_true", help="Generate all scales.")
    parser.add_argument(
        "--local_output_dir",
        default=None,
        help="Write to local directory instead of S3. Files are written to <dir>/<scale>/left.parquet and right.parquet.",
    )
    args = parser.parse_args()

    output_dir = Path(args.local_output_dir) if args.local_output_dir else None
    scales = list(SCALES) if args.all else [args.scale]

    for scale in scales:
        print(f"\n=== {scale} ===")
        generate_scale(scale, output_dir=output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
