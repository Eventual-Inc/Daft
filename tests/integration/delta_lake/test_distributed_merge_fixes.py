"""Regression tests for distributed_merge_deltalake correctness fixes.

Each test targets a specific bug found in review of DistributedDeltaMergeBuilder:

  #4  Unmatched source rows with no (or a failing) insert clause were written
      back as all-NULL rows instead of being dropped.
  #5  Deletes were silently lost on partitioned tables when the affected
      partition had no other (insert/update) modification.
  #8  Multiple when_matched_update clauses were applied last-wins instead of
      SQL MERGE's first-match-wins.
  #9  num_target_rows_deleted was hardcoded to 0.
  #13 Matched-but-not-updated rows were miscounted as "updated" instead of
      "copied".
  #7  Non-equality (residual) predicate conditions silently corrupted rows.
  #11 Duplicate join keys in the source silently produced a row cross-product.
"""

from __future__ import annotations

import pytest

import daft
from daft import col, lit


def _read_sorted(path: str, sort_col: str = "id") -> dict:
    return daft.read_deltalake(path).sort(sort_col).to_pydict()


# ---------------------------------------------------------------------------
# #4 — unmatched source rows must be dropped, not written as NULL rows
# ---------------------------------------------------------------------------


def test_unmatched_source_dropped_when_no_insert_clause(tmp_path):
    """A source row that matches no target and has no insert clause is ignored."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1, 2], "val": ["a", "b"]}).write_deltalake(path)

    # id=3 is unmatched; with only an update clause it must NOT be inserted.
    source = daft.from_pydict({"id": [2, 3], "val": ["B", "C"]})

    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2]  # id=3 dropped, no spurious NULL row
    assert rows["val"] == ["a", "B"]
    assert None not in rows["id"]


def test_unmatched_source_dropped_when_insert_predicate_filters(tmp_path):
    """Only source rows passing the insert predicate are inserted; others dropped."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1], "val": ["a"]}).write_deltalake(path)

    source = daft.from_pydict({"id": [2, 3], "val": ["x", "y"]})

    builder = daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
    s = builder.source_col
    (
        builder.when_not_matched_insert(
            updates={"id": s("id"), "val": s("val")},
            predicate=(s("id") == lit(3)),  # only id=3 inserted
        ).execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 3]  # id=2 filtered out, not a NULL row
    assert rows["val"] == ["a", "y"]
    assert None not in rows["id"]


# ---------------------------------------------------------------------------
# #5 — deletes must survive on partitioned tables even in "untouched" partitions
# ---------------------------------------------------------------------------


def test_matched_delete_on_partitioned_table(tmp_path):
    """when_matched_delete removes rows even when its partition has no other change."""
    path = str(tmp_path / "table")
    daft.from_pydict(
        {"id": [1, 2], "region": ["east", "west"], "val": ["a", "b"]}
    ).write_deltalake(path, partition_cols=["region"])

    # Only id=1 (east) is matched+deleted; west partition is otherwise untouched.
    source = daft.from_pydict({"id": [1], "region": ["east"], "val": ["a"]})

    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_delete()
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [2]  # id=1 actually removed
    assert rows["region"] == ["west"]


def test_by_source_delete_on_partitioned_table(tmp_path):
    """when_not_matched_by_source_delete removes target-only rows in their partition."""
    path = str(tmp_path / "table")
    daft.from_pydict(
        {"id": [1, 2, 3], "region": ["east", "west", "west"], "val": ["a", "b", "c"]}
    ).write_deltalake(path, partition_cols=["region"])

    # Source only has id=1 (east); ids 2,3 (west) are target-only and deleted.
    source = daft.from_pydict({"id": [1], "region": ["east"], "val": ["a"]})

    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_not_matched_by_source_delete()
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1]  # ids 2,3 removed from the west partition
    assert rows["region"] == ["east"]


def test_partitioned_upsert_roundtrip(tmp_path):
    """Full overwrite of a partitioned table handles update + insert correctly."""
    path = str(tmp_path / "table")
    daft.from_pydict(
        {"id": [1, 2], "region": ["east", "west"], "val": ["a", "b"]}
    ).write_deltalake(path, partition_cols=["region"])

    source = daft.from_pydict({"id": [2, 3], "region": ["west", "east"], "val": ["B", "c"]})

    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3]
    assert rows["region"] == ["east", "west", "east"]
    assert rows["val"] == ["a", "B", "c"]


# ---------------------------------------------------------------------------
# #8 — matched update clauses evaluate first-match-wins
# ---------------------------------------------------------------------------


def test_matched_update_clause_precedence_first_match_wins(tmp_path):
    """When two matched-update clauses both apply, the first one wins (SQL MERGE)."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1], "tier": ["base"]}).write_deltalake(path)

    source = daft.from_pydict({"id": [1], "score": [5]})

    builder = daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
    s = builder.source_col
    (
        builder.when_matched_update(predicate=(s("score") >= lit(0)), updates={"tier": lit("A")})  # matches first
        .when_matched_update(predicate=(s("score") >= lit(3)), updates={"tier": lit("B")})  # also matches
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["tier"] == ["A"]  # first matching clause wins, not "B"


# ---------------------------------------------------------------------------
# #9 / #13 — metrics correctness
# ---------------------------------------------------------------------------


def test_matched_not_updated_counts_as_copied(tmp_path):
    """A matched row whose update predicate is false counts as copied, not updated."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]}).write_deltalake(path)

    # id=1 same value (not updated), id=2 changed (updated), id=4 new (inserted).
    source = daft.from_pydict({"id": [1, 2, 4], "val": ["a", "B", "d"]})

    builder = daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
    s = builder.source_col
    t = builder.target_col
    result = (
        builder.when_matched_update(predicate=(s("val") != t("val")), updates={"val": s("val")})
        .when_not_matched_insert(updates={"id": s("id"), "val": s("val")})
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3, 4]
    assert rows["val"] == ["a", "B", "c", "d"]

    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_updated"] == 1  # only id=2
    assert metrics["num_target_rows_inserted"] == 1  # id=4
    assert metrics["num_target_rows_copied"] == 2  # id=1 (matched, unchanged) + id=3 (target-only)


def test_delete_metrics_counted(tmp_path):
    """num_target_rows_deleted reflects actual deletions."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]}).write_deltalake(path)

    source = daft.from_pydict({"id": [2], "val": ["b"]})

    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_delete()
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 3]

    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_deleted"] == 1


# ---------------------------------------------------------------------------
# #7 — residual (non-equi) predicate conditions must fail fast
# ---------------------------------------------------------------------------


def test_residual_predicate_raises(tmp_path):
    """A non-equality predicate condition is rejected with a clear error."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1], "ts": [10]}).write_deltalake(path)

    source = daft.from_pydict({"id": [1], "ts": [20]})

    with pytest.raises((ValueError, NotImplementedError), match="residual|equi|equality"):
        daft.distributed_merge_deltalake(
            table=path,
            source=source,
            predicate="target.id = source.id AND source.ts > target.ts",
        )


# ---------------------------------------------------------------------------
# #11 — duplicate join keys in the source must fail fast
# ---------------------------------------------------------------------------


def test_duplicate_source_keys_raises(tmp_path):
    """Duplicate join keys in the source are rejected instead of exploding rows."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1], "val": ["a"]}).write_deltalake(path)

    source = daft.from_pydict({"id": [1, 1], "val": ["a", "b"]})

    with pytest.raises((ValueError, RuntimeError), match="duplicate|multiple source"):
        (
            daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
