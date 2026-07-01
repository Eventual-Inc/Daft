"""Tests for distributed_merge_deltalake — the distributed join-based merge API.

Tests cover:
  - update_all / insert_all (string-based)
  - custom updates with string expressions
  - native Daft Expression-based predicates and updates (source_col / target_col)
  - when_not_matched_by_source_update
  - when_matched_delete / when_not_matched_by_source_delete
  - merge metrics correctness
  - data verification via read-back
"""

from __future__ import annotations

import pytest

import daft
from daft import DataType, col, lit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_sorted(path: str, sort_col: str = "id") -> dict:
    """Read a Delta table back and return sorted pydict."""
    return daft.read_deltalake(path).sort(sort_col).to_pydict()


# ---------------------------------------------------------------------------
# 1. update_all + insert_all (string predicates)
# ---------------------------------------------------------------------------

def test_distributed_merge_update_all_insert_all(tmp_path):
    """Basic upsert: update existing rows and insert new ones."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1, 2], "value": ["old-a", "old-b"]}).write_deltalake(path)

    source = daft.from_pydict({"id": [2, 3], "value": ["new-b", "new-c"]})

    result = (
        daft.distributed_merge_deltalake(
            table=path,
            source=source,
            predicate="target.id = source.id",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    rows = _read_sorted(path)
    assert rows == {"id": [1, 2, 3], "value": ["old-a", "new-b", "new-c"]}

    # Check metrics
    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_updated"] == 1   # id=2 updated
    assert metrics["num_target_rows_inserted"] == 1  # id=3 inserted
    assert metrics["num_target_rows_copied"] == 1    # id=1 unchanged


# ---------------------------------------------------------------------------
# 2. Custom string-based updates + predicate
# ---------------------------------------------------------------------------

def test_distributed_merge_string_predicate_and_updates(tmp_path):
    """String-based custom predicate and update expressions."""
    path = str(tmp_path / "table")
    daft.from_pydict({
        "id": [1, 2],
        "attrs": ["a", "b"],
        "status": ["ADDED", "ADDED"],
    }).write_deltalake(path)

    source = daft.from_pydict({
        "id": [1, 2, 3],
        "attrs": ["a-new", "b", "c-new"],  # id=1 changed, id=2 same, id=3 new
    })

    result = (
        daft.distributed_merge_deltalake(
            table=path,
            source=source,
            predicate="target.id = source.id",
        )
        .when_matched_update(
            predicate="source.attrs != target.attrs",
            updates={"attrs": "source.attrs", "status": "'UPDATED'"},
        )
        .when_not_matched_insert(
            updates={
                "id": "source.id",
                "attrs": "source.attrs",
                "status": "'ADDED'",
            },
        )
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3]
    assert rows["attrs"] == ["a-new", "b", "c-new"]
    assert rows["status"][0] == "UPDATED"  # id=1 changed
    assert rows["status"][1] == "ADDED"    # id=2 unchanged — kept original
    assert rows["status"][2] == "ADDED"    # id=3 inserted


# ---------------------------------------------------------------------------
# 3. Native Expression-based API (source_col / target_col)
# ---------------------------------------------------------------------------

def test_distributed_merge_native_expressions(tmp_path):
    """Predicates and updates using native Daft Expressions via source_col/target_col."""
    path = str(tmp_path / "table")
    daft.from_pydict({
        "id": [1, 2, 3],
        "hash": ["h1", "h2", "h3"],
        "value": [10, 20, 30],
    }).write_deltalake(path)

    source = daft.from_pydict({
        "id": [1, 2, 4],
        "hash": ["h1-new", "h2", "h4"],
        "value": [11, 20, 40],
    })

    builder = daft.distributed_merge_deltalake(
        table=path,
        source=source,
        predicate="target.id = source.id",
    )

    s = builder.source_col
    t = builder.target_col

    result = (
        builder
        .when_matched_update(
            predicate=(s("hash") != t("hash")),
            updates={
                "hash": s("hash"),
                "value": s("value"),
            },
        )
        .when_not_matched_insert(
            updates={
                "id": s("id"),
                "hash": s("hash"),
                "value": s("value"),
            },
        )
        .execute()
    )

    rows = _read_sorted(path)
    # id=1: hash changed → updated
    # id=2: hash same → NOT updated (kept original)
    # id=3: target-only → copied as-is
    # id=4: source-only → inserted
    assert rows["id"] == [1, 2, 3, 4]
    assert rows["hash"] == ["h1-new", "h2", "h3", "h4"]
    assert rows["value"] == [11, 20, 30, 40]


def test_distributed_merge_native_expr_not_matched_by_source(tmp_path):
    """Native expressions with when_not_matched_by_source_update."""
    path = str(tmp_path / "table")
    daft.from_pydict({
        "id": [1, 2, 3],
        "active": [True, True, True],
    }).write_deltalake(path)

    # Source only has id 1 and 2 — id 3 is missing
    source = daft.from_pydict({"id": [1, 2], "active": [True, True]})

    builder = daft.distributed_merge_deltalake(
        table=path,
        source=source,
        predicate="target.id = source.id",
    )

    result = (
        builder
        .when_not_matched_by_source_update(
            updates={"active": lit(False)},
        )
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3]
    assert rows["active"] == [True, True, False]  # id=3 marked inactive


# ---------------------------------------------------------------------------
# 4. Mixed: string predicate + native expression updates
# ---------------------------------------------------------------------------

def test_distributed_merge_mixed_string_and_expression(tmp_path):
    """String predicate with Expression update values."""
    path = str(tmp_path / "table")
    daft.from_pydict({
        "id": [1, 2],
        "score": [100, 200],
    }).write_deltalake(path)

    source = daft.from_pydict({"id": [1, 2], "score": [150, 250]})

    builder = daft.distributed_merge_deltalake(
        table=path,
        source=source,
        predicate="target.id = source.id",
    )

    s = builder.source_col

    result = (
        builder
        .when_matched_update(
            predicate="source.score != target.score",  # string predicate
            updates={"score": s("score")},             # native expression update
        )
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2]
    assert rows["score"] == [150, 250]


# ---------------------------------------------------------------------------
# 5. Delete clauses
# ---------------------------------------------------------------------------

def test_distributed_merge_matched_delete(tmp_path):
    """when_matched_delete removes rows that exist in both source and target."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]}).write_deltalake(path)

    source = daft.from_pydict({"id": [2], "val": ["b"]})

    result = (
        daft.distributed_merge_deltalake(
            table=path,
            source=source,
            predicate="target.id = source.id",
        )
        .when_matched_delete()
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 3]
    assert rows["val"] == ["a", "c"]


def test_distributed_merge_not_matched_by_source_delete(tmp_path):
    """when_not_matched_by_source_delete removes target-only rows."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]}).write_deltalake(path)

    # Source only has id=1 — ids 2 and 3 missing
    source = daft.from_pydict({"id": [1], "val": ["a"]})

    result = (
        daft.distributed_merge_deltalake(
            table=path,
            source=source,
            predicate="target.id = source.id",
        )
        .when_not_matched_by_source_delete()
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1]
    assert rows["val"] == ["a"]


# ---------------------------------------------------------------------------
# 6. Metrics
# ---------------------------------------------------------------------------

def test_distributed_merge_metrics(tmp_path):
    """Verify merge metrics are computed correctly."""
    path = str(tmp_path / "table")
    daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]}).write_deltalake(path)

    source = daft.from_pydict({"id": [2, 4], "val": ["b-new", "d"]})

    result = (
        daft.distributed_merge_deltalake(
            table=path,
            source=source,
            predicate="target.id = source.id",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_updated"] == 1   # id=2
    assert metrics["num_target_rows_inserted"] == 1  # id=4
    assert metrics["num_target_rows_copied"] == 2    # id=1, id=3

    # Also verify the result is a proper DataFrame with metric columns
    result_dict = result.to_pydict()
    assert result_dict["num_target_rows_updated"] == [1]
    assert result_dict["num_target_rows_inserted"] == [1]
    assert result_dict["num_target_rows_copied"] == [2]


# ---------------------------------------------------------------------------
# 7. Full lifecycle (insert + update + mark-unsourced) in one pass
# ---------------------------------------------------------------------------

def test_distributed_merge_full_lifecycle_native(tmp_path):
    """End-to-end lifecycle merge using native expressions: insert, update, and mark-unsourced."""
    path = str(tmp_path / "table")

    # Target: 3 entities, all sourced
    daft.from_pydict({
        "entity_id": [1, 2, 3],
        "attrs": ["a", "b", "c"],
        "is_sourced": [True, True, True],
    }).write_deltalake(path)

    # Source: entity 1 changed, entity 2 same, entity 3 MISSING, entity 4 NEW
    source = daft.from_pydict({
        "entity_id": [1, 2, 4],
        "attrs": ["a-new", "b", "d"],
    })

    builder = daft.distributed_merge_deltalake(
        table=path,
        source=source,
        predicate="target.entity_id = source.entity_id",
    )

    s = builder.source_col
    t = builder.target_col

    result = (
        builder
        .when_matched_update(
            predicate=(s("attrs") != t("attrs")),
            updates={"attrs": s("attrs"), "is_sourced": lit(True)},
        )
        .when_not_matched_insert(
            updates={
                "entity_id": s("entity_id"),
                "attrs": s("attrs"),
                "is_sourced": lit(True),
            },
        )
        .when_not_matched_by_source_update(
            updates={"is_sourced": lit(False)},
        )
        .execute()
    )

    rows = _read_sorted(path, sort_col="entity_id")
    assert rows["entity_id"] == [1, 2, 3, 4]
    assert rows["attrs"] == ["a-new", "b", "c", "d"]
    assert rows["is_sourced"] == [True, True, False, True]

    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_updated"] >= 1   # id=1 updated + id=3 unsourced
    assert metrics["num_target_rows_inserted"] == 1  # id=4
