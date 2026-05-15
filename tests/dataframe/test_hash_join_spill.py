"""Tests for grace hash join spill-to-disk functionality.

These tests exercise the `hash_join_spill_threshold_bytes` config option which
triggers the grace hash join algorithm: build side is partitioned by hash and
any partition exceeding the per-partition threshold is spilled to Arrow IPC
files.  The probe side buffers rows for spilled partitions and joins them during
finalize_probe.

Run with:
    DAFT_RUNNER=native pytest tests/dataframe/test_hash_join_spill.py -v
"""
from __future__ import annotations

import tempfile
from contextlib import contextmanager

import pytest

import daft
from daft import col
from daft.datatype import DataType



# ─── helpers ──────────────────────────────────────────────────────────────────

@contextmanager
def spill_ctx(threshold_bytes: int = 1):
    """Set a very low spill threshold so every join is forced to spill."""
    with tempfile.TemporaryDirectory() as tmp:
        with daft.execution_config_ctx(
            hash_join_spill_threshold_bytes=threshold_bytes,
            flight_shuffle_dirs=[tmp],
        ):
            yield


def _sort(df: "daft.DataFrame", *keys: str) -> list[dict]:
    return sorted(df.collect().to_pydict_list(), key=lambda r: tuple(r[k] for k in keys))


# ─── basic correctness ────────────────────────────────────────────────────────


def test_inner_join_spill_small():
    """Inner join: {1,2,3} ⋈ {2,3,4} → 2 rows."""
    left = daft.from_pydict({"id": [1, 2, 3], "lv": ["a", "b", "c"]})
    right = daft.from_pydict({"id": [2, 3, 4], "rv": ["x", "y", "z"]})
    with spill_ctx():
        result = left.join(right, on="id").sort("id").collect().to_pydict()
    assert result["id"] == [2, 3]
    assert result["lv"] == ["b", "c"]
    assert result["rv"] == ["x", "y"]


def test_inner_join_spill_no_match():
    """Inner join with disjoint keys → 0 rows."""
    left = daft.from_pydict({"id": [1, 2], "v": [10, 20]})
    right = daft.from_pydict({"id": [3, 4], "v": [30, 40]})
    with spill_ctx():
        result = left.join(right, on="id", how="inner").collect().to_pydict()
    assert result["id"] == []


def test_inner_join_spill_all_match():
    """Inner join where every key matches."""
    ids = list(range(50))
    left = daft.from_pydict({"id": ids, "lv": ids})
    right = daft.from_pydict({"id": ids, "rv": [i * 2 for i in ids]})
    with spill_ctx():
        result = left.join(right, on="id").sort("id").collect().to_pydict()
    assert result["id"] == ids
    assert result["rv"] == [i * 2 for i in ids]


def test_left_join_spill():
    """Left join: all left rows preserved, unmatched right columns are null."""
    left = daft.from_pydict({"id": [1, 2, 3], "lv": ["a", "b", "c"]})
    right = daft.from_pydict({"id": [2, 3, 4], "rv": ["x", "y", "z"]})
    with spill_ctx():
        result = left.join(right, on="id", how="left").sort("id").collect().to_pydict()
    assert result["id"] == [1, 2, 3]
    assert result["rv"] == [None, "x", "y"]


def test_right_join_spill():
    """Right join: all right rows preserved, unmatched left columns are null."""
    left = daft.from_pydict({"id": [1, 2, 3], "lv": ["a", "b", "c"]})
    right = daft.from_pydict({"id": [2, 3, 4], "rv": ["x", "y", "z"]})
    with spill_ctx():
        result = left.join(right, on="id", how="right").sort("id").collect().to_pydict()
    assert result["id"] == [2, 3, 4]
    assert result["lv"] == ["b", "c", None]


def test_outer_join_spill():
    """Outer join: all rows from both sides appear."""
    left = daft.from_pydict({"id": [1, 2, 3], "lv": ["a", "b", "c"]})
    right = daft.from_pydict({"id": [2, 3, 4], "rv": ["x", "y", "z"]})
    with spill_ctx():
        result = left.join(right, on="id", how="outer").sort("id").collect().to_pydict()
    assert result["id"] == [1, 2, 3, 4]


def test_semi_join_spill():
    """Semi join: only left rows with a match."""
    left = daft.from_pydict({"id": [1, 2, 3], "v": [10, 20, 30]})
    right = daft.from_pydict({"id": [2, 3, 4]})
    with spill_ctx():
        result = left.join(right, on="id", how="semi").sort("id").collect().to_pydict()
    assert result["id"] == [2, 3]


def test_anti_join_spill():
    """Anti join: only left rows without a match."""
    left = daft.from_pydict({"id": [1, 2, 3], "v": [10, 20, 30]})
    right = daft.from_pydict({"id": [2, 3, 4]})
    with spill_ctx():
        result = left.join(right, on="id", how="anti").sort("id").collect().to_pydict()
    assert result["id"] == [1]


# ─── larger data ──────────────────────────────────────────────────────────────


def test_inner_join_spill_larger():
    """100 rows each side, 50 matching keys — forces multiple spill partitions."""
    n = 100
    left_ids = list(range(n))
    right_ids = list(range(n // 2, n + n // 2))
    left = daft.from_pydict({"id": left_ids, "lv": left_ids})
    right = daft.from_pydict({"id": right_ids, "rv": right_ids})
    expected_ids = sorted(set(left_ids) & set(right_ids))
    with spill_ctx(threshold_bytes=1):
        result = left.join(right, on="id").sort("id").collect().to_pydict()
    assert result["id"] == expected_ids


# ─── type coverage ────────────────────────────────────────────────────────────


def test_inner_join_spill_string_key():
    """String join key spills correctly."""
    left = daft.from_pydict({"k": ["apple", "banana", "cherry"], "lv": [1, 2, 3]})
    right = daft.from_pydict({"k": ["banana", "cherry", "date"], "rv": [20, 30, 40]})
    with spill_ctx():
        result = left.join(right, on="k").sort("k").collect().to_pydict()
    assert result["k"] == ["banana", "cherry"]
    assert result["lv"] == [2, 3]
    assert result["rv"] == [20, 30]


def test_inner_join_spill_multi_key():
    """Compound join key spills correctly."""
    left = daft.from_pydict({"a": [1, 1, 2], "b": ["x", "y", "x"], "lv": [10, 20, 30]})
    right = daft.from_pydict({"a": [1, 2, 2], "b": ["x", "x", "z"], "rv": [100, 300, 400]})
    with spill_ctx():
        result = left.join(right, on=["a", "b"]).sort([col("a"), col("b")]).collect().to_pydict()
    assert result["a"] == [1, 2]
    assert result["b"] == ["x", "x"]
    assert result["lv"] == [10, 30]
    assert result["rv"] == [100, 300]


# ─── spill disabled (regression guard) ───────────────────────────────────────


def test_inner_join_no_spill_baseline():
    """Sanity check: same query without spill enabled returns the same result."""
    left = daft.from_pydict({"id": [1, 2, 3], "lv": ["a", "b", "c"]})
    right = daft.from_pydict({"id": [2, 3, 4], "rv": ["x", "y", "z"]})
    result = left.join(right, on="id").sort("id").collect().to_pydict()
    assert result["id"] == [2, 3]
    assert result["lv"] == ["b", "c"]
    assert result["rv"] == ["x", "y"]


# ─── threshold boundary ───────────────────────────────────────────────────────


def test_spill_threshold_high_no_actual_spill():
    """With a threshold well above the data size, no data actually spills but
    the multi-partition code path (partition_count=2) is still exercised."""
    left = daft.from_pydict({"id": [1, 2, 3], "lv": ["a", "b", "c"]})
    right = daft.from_pydict({"id": [2, 3, 4], "rv": ["x", "y", "z"]})
    # 1 MiB → partition_count=2; 3-row tables are tiny so nothing spills.
    with spill_ctx(threshold_bytes=1024 * 1024):
        result = left.join(right, on="id").sort("id").collect().to_pydict()
    assert result["id"] == [2, 3]
