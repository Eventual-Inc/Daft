from __future__ import annotations

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa

import daft
from daft import col
from daft.io.delta_lake._deltalake import distributed_merge_deltalake
from tests.conftest import get_tests_daft_runner_name


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


# Module-level so a use_process=False function (same process on the native runner)
# can record how many times the source plan actually executes.
_SOURCE_EXEC = {"n": 0}


@daft.func(return_dtype=daft.DataType.int64(), use_process=False)
def _counting_passthrough(x: int) -> int:
    """Increment a module counter once per source-plan execution, then pass the value
    through unchanged. Row-wise over a single-row source, so one call == one execution.
    Kept int64 so it feeds a risky Int64->Int32 narrowing cast."""
    _SOURCE_EXEC["n"] += 1
    return x


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="Ray always runs UDFs in a separate process, so a module-level counter is not visible.",
)
def test_guard_does_not_re_execute_the_materialized_source(tmp_path):
    """Regression: the cast guard must probe the *materialized* source, not re-run the
    source plan. With a risky cast present and materialize_source=True (default), the
    source must execute exactly ONCE. Before the guard was reordered to run after
    materialization it executed twice (once for the guard's probe, once to collect),
    which both doubled UDF work and — for a non-deterministic source — vetted a
    different execution than the one written, reopening the silent-null bug."""
    # Target `v` is int32; the source `v` is int64 and in range, so the cast is risky
    # (not provably lossless) yet the merge succeeds.
    daft.from_arrow(
        pa.table({"k": pa.array([1], pa.int64()), "v": pa.array([10], pa.int32())})
    ).write_deltalake(str(tmp_path))

    source = daft.from_pydict({"k": [2], "v_raw": [42]}).select(
        col("k"), _counting_passthrough(col("v_raw")).alias("v")
    )

    _SOURCE_EXEC["n"] = 0
    builder = distributed_merge_deltalake(
        str(tmp_path), source, predicate="source.k = target.k", on="k", materialize_source=True
    )
    builder.when_matched_update_all().when_not_matched_insert_all().execute()

    assert _SOURCE_EXEC["n"] == 1, (
        f"source plan executed {_SOURCE_EXEC['n']} time(s); the guard should reuse the "
        "materialized source, not trigger a second execution."
    )

    # The merge still committed the (in-range) value.
    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("k")
    assert table.column("v").to_pylist() == [10, 42]
