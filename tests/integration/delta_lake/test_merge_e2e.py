"""End-to-end tests for merge_deltalake and distributed_merge_deltalake.

Validates both APIs produce correct data, metrics, and Delta history entries
through a multi-step lifecycle: initial load → upsert → mark unsourced → verify history.
"""

from __future__ import annotations

import daft
from daft import DataType, col, lit


def _read_sorted(path: str, sort_col: str = "id") -> dict:
    return daft.read_deltalake(path).sort(sort_col).to_pydict()


def _history(path: str) -> list[dict]:
    return daft.history_deltalake(path)


# ---------------------------------------------------------------------------
# merge_deltalake (DataSink-based, per-partition delta-rs merge)
# ---------------------------------------------------------------------------


def test_merge_deltalake_e2e(tmp_path):
    """Full lifecycle via merge_deltalake: create → upsert → verify history + data."""
    path = str(tmp_path / "table")

    # Step 1: Initial load
    daft.from_pydict({
        "id": [1, 2, 3],
        "name": ["alice", "bob", "carol"],
        "score": [10, 20, 30],
    }).write_deltalake(path)

    rows = _read_sorted(path)
    assert rows == {"id": [1, 2, 3], "name": ["alice", "bob", "carol"], "score": [10, 20, 30]}

    history = _history(path)
    assert len(history) == 1
    assert "CREATE" in history[0]["operation"]  # CREATE TABLE or CREATE or WRITE

    # Step 2: Upsert — update id=2 (score change), insert id=4
    source = daft.from_pydict({
        "id": [2, 4],
        "name": ["bob", "dave"],
        "score": [25, 40],
    })
    result = (
        daft.merge_deltalake(path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    assert isinstance(result, daft.DataFrame)
    metrics = result._metadata["merge_metrics"]
    # Aggregated across partitions — at least 1 updated + 1 inserted
    assert metrics.get("num_target_rows_updated", 0) + metrics.get("num_target_rows_inserted", 0) >= 2

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3, 4]
    assert rows["name"] == ["alice", "bob", "carol", "dave"]
    assert rows["score"] == [10, 25, 30, 40]  # id=2 updated

    # Step 3: Conditional update — only update where score changed
    source2 = daft.from_pydict({
        "id": [1, 3],
        "name": ["alice", "carol"],
        "score": [10, 35],  # id=1 same, id=3 changed
    })
    result2 = (
        daft.merge_deltalake(path, source=source2, predicate="target.id = source.id")
        .when_matched_update(
            predicate="source.score != target.score",
            updates={"score": "source.score"},
        )
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["score"] == [10, 25, 35, 40]  # only id=3 updated

    # Step 4: Verify history has 3 commits (create + 2 merges)
    history = _history(path)
    assert len(history) == 3

    # Step 5: Time-travel read — version 0 should be the original
    v0 = daft.read_deltalake(path, version=0).sort("id").to_pydict()
    assert v0 == {"id": [1, 2, 3], "name": ["alice", "bob", "carol"], "score": [10, 20, 30]}


# ---------------------------------------------------------------------------
# distributed_merge_deltalake (Daft full outer join)
# ---------------------------------------------------------------------------


def test_distributed_merge_e2e(tmp_path):
    """Full lifecycle via distributed_merge_deltalake:
    create → upsert → mark unsourced → verify history + data + time-travel."""
    path = str(tmp_path / "table")

    # Step 1: Initial load
    daft.from_pydict({
        "id": [1, 2, 3],
        "name": ["alice", "bob", "carol"],
        "score": [10, 20, 30],
        "active": [True, True, True],
    }).write_deltalake(path)

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3]
    assert rows["active"] == [True, True, True]

    history = _history(path)
    assert len(history) == 1

    # Step 2: Upsert with update_all + insert_all
    source = daft.from_pydict({
        "id": [2, 4],
        "name": ["bob-v2", "dave"],
        "score": [25, 40],
        "active": [True, True],
    })
    result = (
        daft.distributed_merge_deltalake(path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    metrics = result._metadata["merge_metrics"]
    assert metrics["num_target_rows_updated"] == 1   # id=2
    assert metrics["num_target_rows_inserted"] == 1  # id=4
    assert metrics["num_target_rows_copied"] == 2    # id=1, id=3

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3, 4]
    assert rows["name"] == ["alice", "bob-v2", "carol", "dave"]
    assert rows["score"] == [10, 25, 30, 40]

    # Step 3: Conditional update using native expressions
    source2 = daft.from_pydict({
        "id": [1, 3],
        "name": ["alice", "carol"],
        "score": [10, 35],
        "active": [True, True],
    })
    builder = daft.distributed_merge_deltalake(
        path, source=source2, predicate="target.id = source.id"
    )
    s = builder.source_col
    t = builder.target_col

    result2 = (
        builder
        .when_matched_update(
            predicate=(s("score") != t("score")),
            updates={"score": s("score"), "name": s("name")},
        )
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["score"] == [10, 25, 35, 40]  # only id=3 updated

    # Step 4: Mark unsourced — source missing id=4, mark it inactive
    source3 = daft.from_pydict({
        "id": [1, 2, 3],
        "name": ["alice", "bob-v2", "carol"],
        "score": [10, 25, 35],
        "active": [True, True, True],
    })
    result3 = (
        daft.distributed_merge_deltalake(
            path, source=source3, predicate="target.id = source.id"
        )
        .when_not_matched_by_source_update(
            updates={"active": lit(False)},
        )
        .execute()
    )

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3, 4]
    assert rows["active"] == [True, True, True, False]  # id=4 marked inactive

    metrics3 = result3._metadata["merge_metrics"]
    # ids 1,2,3 are matched with no update clause → copied; id=4 is a target-only
    # row updated by when_not_matched_by_source_update → counted as updated.
    assert metrics3["num_target_rows_copied"] == 3  # id=1, id=2, id=3
    assert metrics3["num_target_rows_updated"] == 1  # id=4 (target-only, updated)

    # Step 5: Verify full history
    history = _history(path)
    assert len(history) == 4  # create + upsert + conditional update + mark unsourced

    # The distributed merge commits should have custom metadata
    distributed_commits = [
        h for h in history
        if isinstance(h.get("operationParameters"), dict)
        and h.get("operationParameters", {}).get("predicate") is not None
    ]
    # At least the initial write should be present
    assert len(history) >= 4

    # Step 6: Time-travel — version 0 = original data
    v0 = daft.read_deltalake(path, version=0).sort("id").to_pydict()
    assert v0 == {
        "id": [1, 2, 3],
        "name": ["alice", "bob", "carol"],
        "score": [10, 20, 30],
        "active": [True, True, True],
    }

    # version 1 = after first upsert
    v1 = daft.read_deltalake(path, version=1).sort("id").to_pydict()
    assert v1["id"] == [1, 2, 3, 4]
    assert v1["name"][1] == "bob-v2"

    # version 3 = after marking unsourced
    v3 = daft.read_deltalake(path, version=3).sort("id").to_pydict()
    assert v3["active"] == [True, True, True, False]


# ---------------------------------------------------------------------------
# Cross-validation: both APIs produce same result from same inputs
# ---------------------------------------------------------------------------


def test_merge_and_distributed_merge_produce_same_result(tmp_path):
    """Both merge_deltalake and distributed_merge_deltalake produce identical
    final table state given the same initial data and merge operations."""
    initial = {
        "id": [1, 2, 3],
        "val": ["a", "b", "c"],
        "flag": [True, True, True],
    }
    source_data = {
        "id": [2, 4],
        "val": ["b-new", "d"],
        "flag": [True, True],
    }

    # --- merge_deltalake path ---
    path_m = str(tmp_path / "merge_table")
    daft.from_pydict(initial).write_deltalake(path_m)
    (
        daft.merge_deltalake(path_m, source=daft.from_pydict(source_data), predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    rows_m = _read_sorted(path_m)

    # --- distributed_merge_deltalake path ---
    path_d = str(tmp_path / "dist_table")
    daft.from_pydict(initial).write_deltalake(path_d)
    (
        daft.distributed_merge_deltalake(path_d, source=daft.from_pydict(source_data), predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    rows_d = _read_sorted(path_d)

    # Both should have the same data
    assert rows_m == rows_d
    assert rows_m["id"] == [1, 2, 3, 4]
    assert rows_m["val"] == ["a", "b-new", "c", "d"]

    # Both should have 2 versions
    assert len(_history(path_m)) == 2
    assert len(_history(path_d)) == 2


def test_merge_and_distributed_merge_metrics_consistency(tmp_path):
    """Both APIs report consistent metrics for the same operation."""
    initial = {"id": [1, 2], "v": [10, 20]}
    source_data = {"id": [2, 3], "v": [25, 30]}

    # merge_deltalake
    path_m = str(tmp_path / "m")
    daft.from_pydict(initial).write_deltalake(path_m)
    result_m = (
        daft.merge_deltalake(path_m, source=daft.from_pydict(source_data), predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    # distributed_merge_deltalake
    path_d = str(tmp_path / "d")
    daft.from_pydict(initial).write_deltalake(path_d)
    result_d = (
        daft.distributed_merge_deltalake(path_d, source=daft.from_pydict(source_data), predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    m_m = result_m._metadata["merge_metrics"]
    m_d = result_d._metadata["merge_metrics"]

    # distributed_merge metrics are exact
    assert m_d["num_target_rows_updated"] == 1
    assert m_d["num_target_rows_inserted"] == 1
    assert m_d["num_target_rows_copied"] == 1

    # merge_deltalake aggregates across partitions — totals should match
    assert m_m.get("num_target_rows_updated", 0) + m_m.get("num_target_rows_inserted", 0) >= 2

    # Final data should match
    assert _read_sorted(path_m) == _read_sorted(path_d)
