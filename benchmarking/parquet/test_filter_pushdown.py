"""Benchmarks for filter/predicate pushdown at varying selectivities and types.

Generates parquet files with known value distributions and measures read
performance when filtering at 1%, 10%, 50%, and 90% selectivity across
integer, float, and string predicates.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft

NUM_ROWS = 2_000_000
NUM_ROW_GROUPS = 8
# Embed params in directory name so changes force regeneration.
LOCAL_DATA_PATH = Path(__file__).parent / f"local_data/r{NUM_ROWS}_rg{NUM_ROW_GROUPS}"

SELECTIVITIES = [0.01, 0.10, 0.50, 0.90]


def _generate_filter_pushdown_file() -> str:
    """Generate a parquet file with columns designed for predictable filtering.

    - filter_int: uniform in [0, 100), so `filter_int < X` selects ~X% of rows.
    - filter_float: uniform in [0.0, 1.0), so `filter_float < X` selects ~X*100% of rows.
    - filter_str: one of "cat_00" .. "cat_99" (uniform), so `filter_str < "cat_XX"` selects ~XX%.
    - payload columns: additional columns that must be materialized after filtering.
    """
    filepath = LOCAL_DATA_PATH / "filter_pushdown.parquet"
    if filepath.exists():
        return str(filepath)

    LOCAL_DATA_PATH.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(42)
    table = pa.table(
        {
            "filter_int": pa.array(rng.integers(0, 100, size=NUM_ROWS), type=pa.int64()),
            "filter_float": pa.array(rng.uniform(0.0, 1.0, size=NUM_ROWS), type=pa.float64()),
            "filter_str": pa.array(
                [f"cat_{i:02d}" for i in rng.integers(0, 100, size=NUM_ROWS)],
                type=pa.string(),
            ),
            "payload_int": pa.array(rng.integers(0, 1_000_000, size=NUM_ROWS), type=pa.int64()),
            "payload_str": pa.array(
                [f"val_{i}" for i in rng.integers(0, 10_000, size=NUM_ROWS)],
                type=pa.string(),
            ),
            "payload_float": pa.array(rng.standard_normal(NUM_ROWS), type=pa.float64()),
        }
    )
    papq.write_table(table, str(filepath), row_group_size=NUM_ROWS // NUM_ROW_GROUPS)
    return str(filepath)


@pytest.fixture(scope="session")
def filter_file():
    return _generate_filter_pushdown_file()


@pytest.fixture(params=SELECTIVITIES, ids=[f"sel_{int(s*100)}pct" for s in SELECTIVITIES])
def selectivity(request):
    return request.param


def _sel_label(selectivity):
    return f"sel_{int(selectivity * 100)}pct"


# ---------------------------------------------------------------------------
# Integer predicate: filter_int < threshold
# ---------------------------------------------------------------------------

def test_daft_filter_int(filter_file, selectivity, benchmark):
    benchmark.group = f"filter_int_{_sel_label(selectivity)}"
    threshold = int(selectivity * 100)
    benchmark(lambda: daft.read_parquet(filter_file).where(daft.col("filter_int") < threshold).collect())


def test_pyarrow_filter_int(filter_file, selectivity, benchmark):
    benchmark.group = f"filter_int_{_sel_label(selectivity)}"
    threshold = int(selectivity * 100)
    benchmark(lambda: papq.read_table(filter_file, filters=[("filter_int", "<", threshold)]))


# ---------------------------------------------------------------------------
# Float predicate: filter_float < threshold
# ---------------------------------------------------------------------------

def test_daft_filter_float(filter_file, selectivity, benchmark):
    benchmark.group = f"filter_float_{_sel_label(selectivity)}"
    benchmark(lambda: daft.read_parquet(filter_file).where(daft.col("filter_float") < selectivity).collect())


def test_pyarrow_filter_float(filter_file, selectivity, benchmark):
    benchmark.group = f"filter_float_{_sel_label(selectivity)}"
    benchmark(lambda: papq.read_table(filter_file, filters=[("filter_float", "<", selectivity)]))


# ---------------------------------------------------------------------------
# String predicate: filter_str < "cat_XX"
# ---------------------------------------------------------------------------

def test_daft_filter_str(filter_file, selectivity, benchmark):
    benchmark.group = f"filter_str_{_sel_label(selectivity)}"
    threshold = f"cat_{int(selectivity * 100):02d}"
    benchmark(lambda: daft.read_parquet(filter_file).where(daft.col("filter_str") < threshold).collect())


def test_pyarrow_filter_str(filter_file, selectivity, benchmark):
    benchmark.group = f"filter_str_{_sel_label(selectivity)}"
    threshold = f"cat_{int(selectivity * 100):02d}"
    benchmark(lambda: papq.read_table(filter_file, filters=[("filter_str", "<", threshold)]))


# ---------------------------------------------------------------------------
# Compound predicate: filter_int < X AND filter_float < Y
# ---------------------------------------------------------------------------

def test_daft_filter_compound(filter_file, selectivity, benchmark):
    benchmark.group = f"filter_compound_{_sel_label(selectivity)}"
    int_thresh = int(selectivity * 100)
    benchmark(
        lambda: daft.read_parquet(filter_file)
        .where((daft.col("filter_int") < int_thresh) & (daft.col("filter_float") < selectivity))
        .collect()
    )


def test_pyarrow_filter_compound(filter_file, selectivity, benchmark):
    benchmark.group = f"filter_compound_{_sel_label(selectivity)}"
    int_thresh = int(selectivity * 100)
    benchmark(
        lambda: papq.read_table(
            filter_file,
            filters=[("filter_int", "<", int_thresh), ("filter_float", "<", selectivity)],
        )
    )


# ---------------------------------------------------------------------------
# Baseline: no filter
# ---------------------------------------------------------------------------

def test_daft_no_filter_baseline(filter_file, benchmark):
    benchmark.group = "no_filter_baseline"
    benchmark(lambda: daft.read_parquet(filter_file).collect())


def test_pyarrow_no_filter_baseline(filter_file, benchmark):
    benchmark.group = "no_filter_baseline"
    benchmark(lambda: papq.read_table(filter_file))
