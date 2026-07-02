"""Tests for distributed_merge_deltalake performance optimizations.

Covers the incremental (partition-scoped copy-on-write) write path and the
two-pass streaming execution:

  - Merges only rewrite partitions that contain a modification; data files of
    untouched partitions are preserved byte-for-byte (same add_action paths).
  - A merge that modifies nothing skips the commit entirely.
  - Updates that move a row across partitions rewrite both the pre-image and
    post-image partitions.
  - validate_unique_keys=False skips the duplicate-source-key guard.
  - The decomposed (broadcast-friendly left+anti) join produces results
    identical to the plain full-outer join.
"""

from __future__ import annotations

import deltalake
import pyarrow as pa
import pytest

import daft
from daft import col, lit


def _read_sorted(path: str, sort_col: str = "id") -> dict:
    return daft.read_deltalake(path).sort(sort_col).to_pydict()


def _data_files(path: str) -> set[str]:
    """Current live data file paths of a Delta table."""
    dt = deltalake.DeltaTable(path)
    return set(pa.table(dt.get_add_actions()).to_pydict()["path"])


def _version(path: str) -> int:
    return deltalake.DeltaTable(path).version()


def _make_partitioned(path: str) -> None:
    daft.from_pydict(
        {
            "id": [1, 2, 3],
            "region": ["east", "west", "north"],
            "val": ["a", "b", "c"],
        }
    ).write_deltalake(path, partition_cols=["region"])


# ---------------------------------------------------------------------------
# Partition-scoped copy-on-write
# ---------------------------------------------------------------------------


def test_update_only_rewrites_affected_partition(tmp_path):
    """Updating a row in one partition leaves other partitions' files untouched."""
    path = str(tmp_path / "table")
    _make_partitioned(path)
    before = _data_files(path)

    source = daft.from_pydict({"id": [1], "region": ["east"], "val": ["A2"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .execute()
    )

    after = _data_files(path)
    untouched = {f for f in before if "region=west" in f or "region=north" in f}
    assert untouched <= after  # west/north files preserved as-is
    assert not any("region=east" in f and f in before for f in after)  # east replaced

    rows = _read_sorted(path)
    assert rows["val"] == ["A2", "b", "c"]


def test_insert_only_touches_new_partition(tmp_path):
    """Inserting rows into a new partition preserves all existing files."""
    path = str(tmp_path / "table")
    _make_partitioned(path)
    before = _data_files(path)

    source = daft.from_pydict({"id": [4], "region": ["south"], "val": ["d"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    after = _data_files(path)
    assert before <= after  # every pre-existing file preserved
    assert _read_sorted(path)["id"] == [1, 2, 3, 4]


def test_delete_rewrites_only_its_partition(tmp_path):
    """A matched delete rewrites only the deleted row's partition."""
    path = str(tmp_path / "table")
    _make_partitioned(path)
    before = _data_files(path)

    source = daft.from_pydict({"id": [1], "region": ["east"], "val": ["a"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_delete()
        .execute()
    )

    after = _data_files(path)
    untouched = {f for f in before if "region=west" in f or "region=north" in f}
    assert untouched <= after
    assert not any("region=east" in f for f in after)  # east emptied

    rows = _read_sorted(path)
    assert rows["id"] == [2, 3]


def test_partition_migration_rewrites_both_partitions(tmp_path):
    """An update that changes a row's partition value rewrites source and destination partitions."""
    path = str(tmp_path / "table")
    _make_partitioned(path)
    before = _data_files(path)

    # Move id=1 from east to west (and change val)
    source = daft.from_pydict({"id": [1], "region": ["west"], "val": ["a2"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .execute()
    )

    after = _data_files(path)
    # north untouched; east and west both rewritten
    north = {f for f in before if "region=north" in f}
    assert north <= after
    assert not any(f in after for f in before if "region=east" in f or "region=west" in f)

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3]
    assert rows["region"] == ["west", "west", "north"]  # id=1 migrated, exactly once
    assert rows["val"] == ["a2", "b", "c"]


def test_noop_merge_skips_commit(tmp_path):
    """A merge that modifies nothing does not create a new table version."""
    path = str(tmp_path / "table")
    _make_partitioned(path)
    v_before = _version(path)

    # Matched, but update predicate is false for every row -> nothing modified.
    source = daft.from_pydict({"id": [1, 2], "region": ["east", "west"], "val": ["a", "b"]})
    builder = daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
    s, t = builder.source_col, builder.target_col
    result = builder.when_matched_update(
        predicate=(s("val") != t("val")),  # values identical -> never fires
        updates={"val": s("val")},
    ).execute()

    assert _version(path) == v_before  # no commit
    assert _read_sorted(path)["val"] == ["a", "b", "c"]

    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_updated"] == 0
    assert metrics["num_target_rows_inserted"] == 0
    assert metrics["num_target_rows_deleted"] == 0
    assert metrics["num_target_rows_copied"] == 0  # nothing rewritten


def test_unpartitioned_noop_merge_skips_commit(tmp_path):
    """No-op skip also applies to unpartitioned tables."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1, 2], "val": ["a", "b"]}).write_deltalake(path)
    v_before = _version(path)

    source = daft.from_pydict({"id": [1], "val": ["a"]})
    builder = daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
    s, t = builder.source_col, builder.target_col
    builder.when_matched_update(
        predicate=(s("val") != t("val")),
        updates={"val": s("val")},
    ).execute()

    assert _version(path) == v_before
    assert _read_sorted(path)["val"] == ["a", "b"]


# ---------------------------------------------------------------------------
# Guard opt-out
# ---------------------------------------------------------------------------


def test_validate_unique_keys_opt_out(tmp_path):
    """validate_unique_keys=False skips the duplicate-source-key check."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1], "val": ["a"]}).write_deltalake(path)

    dup = daft.from_pydict({"id": [2, 2], "val": ["x", "y"]})
    # With the guard off, duplicate keys are the caller's responsibility;
    # the merge must not raise the duplicate-key ValueError.
    (
        daft.distributed_merge_deltalake(
            table=path,
            source=dup,
            predicate="target.id = source.id",
            validate_unique_keys=False,
        )
        .when_not_matched_insert_all()
        .execute()
    )
    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 2]  # both duplicates inserted (caller's choice)


# ---------------------------------------------------------------------------
# Decomposed (broadcast-friendly) join equivalence
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Large-source safety: no forced materialization, size-aware broadcast
# ---------------------------------------------------------------------------


def test_materialize_source_false_equivalence(tmp_path):
    """materialize_source=False streams the source (no collect) with identical results."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]}).write_deltalake(path)

    source = daft.from_pydict({"id": [2, 4], "val": ["B", "d"]})
    result = (
        daft.distributed_merge_deltalake(
            table=path,
            source=source,
            predicate="target.id = source.id",
            materialize_source=False,
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3, 4]
    assert rows["val"] == ["a", "B", "c", "d"]

    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_updated"] == 1
    assert metrics["num_target_rows_inserted"] == 1
    assert metrics["num_target_rows_copied"] == 2


def test_broadcast_heuristic_is_size_aware():
    """Auto broadcast decomposition only fires for provably small sources on Ray."""
    from daft.io.delta_lake._deltalake import (
        _BROADCAST_SOURCE_MAX_BYTES,
        _should_decompose_join,
    )

    small = 10 * 1024 * 1024
    huge = 400 * 1024 * 1024 * 1024

    # Explicit override always wins.
    assert _should_decompose_join(True, "native", None) is True
    assert _should_decompose_join(False, "ray", small) is False

    # Auto: never on non-Ray runners (broadcast unsupported there).
    assert _should_decompose_join(None, "native", small) is False

    # Auto on Ray: only when the source is known to be small.
    assert _should_decompose_join(None, "ray", small) is True
    assert _should_decompose_join(None, "ray", huge) is False
    assert _should_decompose_join(None, "ray", None) is False  # unknown size -> unsafe
    assert _should_decompose_join(None, "ray", _BROADCAST_SOURCE_MAX_BYTES + 1) is False


@pytest.mark.parametrize("broadcast", [True, False])
def test_decomposed_join_equivalence(tmp_path, broadcast):
    """The left+anti decomposed join produces identical results to full outer."""
    path = str(tmp_path / "table")
    daft.from_pydict(
        {"id": [1, 2, 3], "val": ["a", "b", "c"], "active": [True, True, True]}
    ).write_deltalake(path)

    # Exercise all three row categories: update id=2, insert id=4, unsource id=1,3? keep simple:
    source = daft.from_pydict({"id": [2, 4], "val": ["B", "d"]})
    builder = daft.distributed_merge_deltalake(
        table=path,
        source=source,
        predicate="target.id = source.id",
        broadcast_join=broadcast,
    )
    s = builder.source_col
    result = (
        builder.when_matched_update(updates={"val": s("val")})
        .when_not_matched_insert(updates={"id": s("id"), "val": s("val"), "active": lit(True)})
        .when_not_matched_by_source_update(updates={"active": lit(False)})
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3, 4]
    assert rows["val"] == ["a", "B", "c", "d"]
    assert rows["active"] == [False, True, False, True]

    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_updated"] == 3  # id=2 updated + id=1,3 by-source
    assert metrics["num_target_rows_inserted"] == 1
