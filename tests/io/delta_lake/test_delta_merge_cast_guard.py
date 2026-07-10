from __future__ import annotations

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa

import daft
from daft.io.delta_lake._deltalake import distributed_merge_deltalake


def _merge(path, source_table):
    builder = distributed_merge_deltalake(
        str(path), daft.from_arrow(source_table), predicate="source.k = target.k", on="k"
    )
    builder.when_matched_update_all().when_not_matched_insert_all().execute()


def test_merge_into_narrower_column_raises_instead_of_nulling(tmp_path):
    """A pre-existing `integer` column cannot hold 3e9. Today the value silently becomes
    NULL and the merge reports success."""
    # Seed a table whose Delta type is `integer` (int32), bypassing Fix 2's widening.
    daft.from_arrow(
        pa.table({"k": pa.array([1], pa.int64()), "v": pa.array([10], pa.int32())})
    ).write_deltalake(str(tmp_path))

    src = pa.table({"k": pa.array([2], pa.int64()), "v": pa.array([3_000_000_000], pa.int64())})
    with pytest.raises(ValueError) as excinfo:
        _merge(tmp_path, src)

    msg = str(excinfo.value)
    assert "v" in msg
    assert "Int64" in msg and "Int32" in msg

    # And nothing was committed beyond the seed.
    assert deltalake.DeltaTable(str(tmp_path)).version() == 0


def test_merge_that_narrows_within_range_still_succeeds(tmp_path):
    """The guard must be data-dependent, not type-dependent: an int64 source whose values
    all fit in an int32 target is a legitimate merge."""
    daft.from_arrow(
        pa.table({"k": pa.array([1], pa.int64()), "v": pa.array([10], pa.int32())})
    ).write_deltalake(str(tmp_path))

    src = pa.table({"k": pa.array([2], pa.int64()), "v": pa.array([42], pa.int64())})
    _merge(tmp_path, src)

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("k")
    assert table.column("v").to_pylist() == [10, 42]


def test_null_source_values_do_not_trip_the_guard(tmp_path):
    daft.from_arrow(
        pa.table({"k": pa.array([1], pa.int64()), "v": pa.array([10], pa.int32())})
    ).write_deltalake(str(tmp_path))

    src = pa.table({"k": pa.array([2], pa.int64()), "v": pa.array([None], pa.int64())})
    _merge(tmp_path, src)

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("k")
    assert table.column("v").to_pylist() == [10, None]


def test_merge_into_widened_column_preserves_the_value(tmp_path):
    """With Fix 2, a uint32 column is declared `long`, so 3e9 needs no narrowing cast."""
    daft.from_arrow(
        pa.table({"k": pa.array([1], pa.int64()), "v": pa.array([10], pa.uint32())})
    ).write_deltalake(str(tmp_path))

    src = pa.table({"k": pa.array([2], pa.int64()), "v": pa.array([3_000_000_000], pa.uint32())})
    _merge(tmp_path, src)

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("k")
    assert table.column("v").to_pylist() == [10, 3_000_000_000]
