"""Edge-case tests for distributed_merge_deltalake.

Targets the two hot code paths in DistributedDeltaMergeBuilder.execute():

  - the materialized first-match clause indices: zero clauses, many clauses
    across all three categories, NULL predicates on insert / by-source
    clauses, except_cols on *_all clauses, and empty sources;
  - the per-column IN partition write filter: bool / int / date partition
    values, single-value (==) vs multi-value (IN) branches, three-column
    non-product closures, many-partition deletes, insert-only new partitions,
    and the NULL-partition-value full-overwrite fallback.
"""

from __future__ import annotations

import datetime

import deltalake
import pyarrow as pa
import pytest

import daft


def _read_sorted(path: str, sort_col: str = "id") -> dict:
    return daft.read_deltalake(path).sort(sort_col).to_pydict()


def _data_files(path: str) -> set[str]:
    """Current live data file paths of a Delta table."""
    dt = deltalake.DeltaTable(path)
    return set(pa.table(dt.get_add_actions()).to_pydict()["path"])


def _version(path: str) -> int:
    return deltalake.DeltaTable(path).version()


# ---------------------------------------------------------------------------
# Clause-index edge cases
# ---------------------------------------------------------------------------


def test_no_clauses_is_noop(tmp_path):
    """A builder with zero WHEN clauses modifies nothing and skips the commit."""
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1, 2], "v": ["a", "b"]}).write_deltalake(path)
    v0 = _version(path)
    source = daft.from_pydict({"id": [1, 3], "v": ["A", "C"]})
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .execute()
        .to_pydict()
    )
    assert _version(path) == v0
    assert result["num_target_rows_inserted"][0] == 0
    assert result["num_target_rows_updated"][0] == 0
    assert result["num_target_rows_deleted"][0] == 0
    assert result["num_output_rows"][0] == 0
    assert _read_sorted(path)["v"] == ["a", "b"]


def test_many_clauses_first_match_wins_across_all_categories(tmp_path):
    """Seven clauses spanning matched / insert / by-source, one row per outcome."""
    path = str(tmp_path / "t")
    daft.from_pydict(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "v": [0, 2, 3, 4, 5, 6],
            "tag": ["t1", "t2", "t3", "t4", "t5", "t6"],
            "stale": [False, False, False, False, True, False],
        }
    ).write_deltalake(path)
    source = daft.from_pydict(
        {
            "id": [1, 2, 3, 4, 7, 8],
            "v": [200, -5, 50, 5, 10, -1],
            "tag": ["s1", "s2", "s3", "s4", "s7", "s8"],
        }
    )
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update({"tag": "'big'"}, predicate="source.v > 100")
        .when_matched_delete(predicate="source.v < 0")
        .when_matched_update_all(predicate="source.v >= 10")
        .when_not_matched_insert(
            {"id": "source.id", "v": "source.v", "tag": "'seven'"}, predicate="source.v > 0"
        )
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete(predicate="target.stale = true")
        .when_not_matched_by_source_update({"tag": "'kept'"})
        .execute()
    ).to_pydict()

    rows = _read_sorted(path)
    # id=1: first matched clause wins over update_all -> only tag changes.
    # id=2: matched delete. id=3: update_all. id=4: no matched clause -> copied.
    # id=5: by-source delete. id=6: by-source update.
    # id=7: first insert clause. id=8: falls through to insert_all.
    assert rows["id"] == [1, 3, 4, 6, 7, 8]
    assert rows["v"] == [0, 50, 4, 6, 10, -1]
    assert rows["tag"] == ["big", "s3", "t4", "kept", "seven", "s8"]
    assert rows["stale"] == [False, False, False, False, None, None]

    assert result["num_target_rows_inserted"][0] == 2
    assert result["num_target_rows_updated"][0] == 3
    assert result["num_target_rows_deleted"][0] == 2
    assert result["num_target_rows_copied"][0] == 1
    assert result["num_output_rows"][0] == 6


def test_null_insert_predicate_drops_source_row(tmp_path):
    """A NULL-evaluating insert predicate is not-satisfied; the row is dropped."""
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1], "v": [1.0]}).write_deltalake(path)
    source = daft.from_pydict({"id": [2, 3], "v": [None, 7.0]})
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_not_matched_insert_all(predicate="source.v > 5")
        .execute()
    ).to_pydict()
    rows = _read_sorted(path)
    assert rows["id"] == [1, 3]
    assert rows["v"] == [1.0, 7.0]
    assert result["num_target_rows_inserted"][0] == 1


def test_null_by_source_predicate_treated_not_satisfied(tmp_path):
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1, 2], "note": ["x", None], "v": [1, 2]}).write_deltalake(path)
    source = daft.from_pydict({"id": [99], "note": ["z"], "v": [99]})
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_not_matched_by_source_update({"v": "target.v + 10"}, predicate="target.note = 'x'")
        .execute()
    ).to_pydict()
    rows = _read_sorted(path)
    # id=1 fires; id=2's NULL predicate does not; source-only id=99 is dropped.
    assert rows["id"] == [1, 2]
    assert rows["v"] == [11, 2]
    assert result["num_target_rows_updated"][0] == 1


def test_update_all_except_cols(tmp_path):
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1, 2], "v": [1, 2], "audit": ["orig1", "orig2"]}).write_deltalake(path)
    source = daft.from_pydict({"id": [1], "v": [100], "audit": ["new1"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all(except_cols=["audit"])
        .execute()
    )
    rows = _read_sorted(path)
    assert rows["v"] == [100, 2]
    assert rows["audit"] == ["orig1", "orig2"]  # excluded column keeps target value


def test_insert_all_except_cols(tmp_path):
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1], "v": [1], "audit": ["a"]}).write_deltalake(path)
    source = daft.from_pydict({"id": [2], "v": [2], "audit": ["should_not_insert"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_not_matched_insert_all(except_cols=["audit"])
        .execute()
    )
    rows = _read_sorted(path)
    assert rows["id"] == [1, 2]
    assert rows["audit"] == ["a", None]  # excluded column inserted as NULL


@pytest.mark.parametrize("bad_name", ["__daft_dm_midx__", "__daft_dm_src__", "x.__src__"])
def test_reserved_column_names_rejected(tmp_path, bad_name):
    """User columns in the merge-internal namespace fail loudly, not silently."""
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1], bad_name: [10]}).write_deltalake(path)
    source = daft.from_pydict({"id": [1], bad_name: [99]})
    with pytest.raises(ValueError, match="reserved"):
        (
            daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
            .when_matched_update_all()
            .execute()
        )


def test_empty_source_is_noop(tmp_path):
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1, 2], "v": ["a", "b"]}).write_deltalake(path)
    v0 = _version(path)
    source = daft.from_arrow(
        pa.table({"id": pa.array([], pa.int64()), "v": pa.array([], pa.string())})
    )
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    ).to_pydict()
    assert _version(path) == v0  # nothing matched, nothing inserted -> no commit
    assert result["num_source_rows"][0] == 0
    assert result["num_output_rows"][0] == 0
    assert _read_sorted(path)["v"] == ["a", "b"]


# ---------------------------------------------------------------------------
# Partition write-filter edge cases
# ---------------------------------------------------------------------------


def test_bool_partition_column(tmp_path):
    path = str(tmp_path / "t")
    daft.from_pydict(
        {"id": [1, 2, 3, 4], "flag": [True, True, False, False], "v": ["a", "b", "c", "d"]}
    ).write_deltalake(path, partition_cols=["flag"])

    # Single affected value -> equality branch; flag=False files untouched.
    false_files_before = {f for f in _data_files(path) if "flag=false" in f}
    source = daft.from_pydict({"id": [1], "flag": [True], "v": ["A"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .execute()
    )
    assert {f for f in _data_files(path) if "flag=false" in f} == false_files_before
    assert _read_sorted(path)["v"] == ["A", "b", "c", "d"]

    # Both values affected -> IN branch over [False, True].
    source = daft.from_pydict({"id": [2, 3], "flag": [True, False], "v": ["B", "C"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .execute()
    )
    assert _read_sorted(path)["v"] == ["A", "B", "C", "d"]


def test_int_partition_many_values_delete_only(tmp_path):
    path = str(tmp_path / "t")
    ids = list(range(20))
    daft.from_pydict(
        {"id": ids, "bucket": [i % 10 for i in ids], "v": [f"v{i}" for i in ids]}
    ).write_deltalake(path, partition_cols=["bucket"])
    untouched_before = {
        f for f in _data_files(path) if any(f"bucket={b}/" in f for b in range(6, 10))
    }

    source = daft.from_pydict({"id": [0, 1, 2, 3, 4, 5]})
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_delete()
        .execute()
    ).to_pydict()

    rows = _read_sorted(path)
    assert rows["id"] == sorted(set(ids) - {0, 1, 2, 3, 4, 5})
    assert result["num_target_rows_deleted"][0] == 6
    assert result["num_target_rows_copied"][0] == 6  # survivors of buckets 0-5
    untouched_after = {
        f for f in _data_files(path) if any(f"bucket={b}/" in f for b in range(6, 10))
    }
    assert untouched_after == untouched_before


def test_date_partition_multiple_values(tmp_path):
    path = str(tmp_path / "t")
    d1, d2, d3 = datetime.date(2024, 1, 1), datetime.date(2024, 2, 1), datetime.date(2024, 3, 1)
    daft.from_pydict({"id": [1, 2, 3], "d": [d1, d2, d3], "v": ["a", "b", "c"]}).write_deltalake(
        path, partition_cols=["d"]
    )
    untouched_before = {f for f in _data_files(path) if "d=2024-03-01" in f}

    source = daft.from_pydict({"id": [1, 2], "d": [d1, d2], "v": ["A", "B"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .execute()
    )
    rows = _read_sorted(path)
    assert rows["v"] == ["A", "B", "c"]
    assert rows["d"] == [d1, d2, d3]
    assert {f for f in _data_files(path) if "d=2024-03-01" in f} == untouched_before


def test_three_partition_columns_nonproduct_closure(tmp_path):
    """Touched tuples differing in every column expand to their full closure."""
    path = str(tmp_path / "t")
    daft.from_pydict(
        {
            "region": ["east", "west", "east", "west"],
            "env": ["prod", "dev", "dev", "prod"],
            "tier": ["hi", "lo", "lo", "hi"],
            "id": [1, 2, 3, 4],
            "v": ["a", "b", "c", "d"],
        }
    ).write_deltalake(path, partition_cols=["region", "env", "tier"])
    source = daft.from_pydict(
        {"id": [1, 2], "v": ["A", "B"], "region": ["east", "west"], "env": ["prod", "dev"], "tier": ["hi", "lo"]}
    )
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .execute()
    ).to_pydict()
    rows = _read_sorted(path)
    # Closure = {east,west} x {prod,dev} x {hi,lo} covers all four rows: the
    # untouched two are rewritten as copies, none are lost.
    assert rows["id"] == [1, 2, 3, 4]
    assert rows["v"] == ["A", "B", "c", "d"]
    assert result["num_target_rows_copied"][0] == 2


def test_mixed_equality_and_in_filter_branches(tmp_path):
    """One partition column takes the == branch, the other the IN branch."""
    path = str(tmp_path / "t")
    daft.from_pydict(
        {
            "y": [2023, 2023, 2024],
            "m": ["jan", "jun", "jan"],
            "id": [1, 2, 3],
            "v": ["a", "b", "c"],
        }
    ).write_deltalake(path, partition_cols=["y", "m"])
    untouched_before = {f for f in _data_files(path) if "y=2024" in f}

    source = daft.from_pydict({"id": [1, 2], "v": ["A", "B"], "y": [2023, 2023], "m": ["jan", "jun"]})
    (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .execute()
    )
    rows = _read_sorted(path)
    assert rows["v"] == ["A", "B", "c"]
    # (2024, jan) is outside the closure {2023} x {jan, jun}: preserved as-is.
    assert {f for f in _data_files(path) if "y=2024" in f} == untouched_before


def test_null_partition_value_falls_back_to_full_overwrite(tmp_path):
    """An affected NULL partition value cannot be filtered; full overwrite instead."""
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1, 2], "p": ["a", "b"], "v": ["x", "y"]}).write_deltalake(
        path, partition_cols=["p"]
    )
    source = daft.from_pydict({"id": [1, 3], "p": ["a", None], "v": ["X", "Z"]})
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    ).to_pydict()
    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3]
    assert rows["p"] == ["a", "b", None]
    assert rows["v"] == ["X", "y", "Z"]
    assert result["num_target_rows_inserted"][0] == 1
    assert result["num_target_rows_updated"][0] == 1


# ---------------------------------------------------------------------------
# Write-pass pruning invariants + materialize_join
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("broadcast", [None, True])
def test_match_in_unaffected_partition_not_duplicated(tmp_path, broadcast):
    """A source row matched in an untouched partition must not be re-inserted.

    id=2's target row lives in partition b (untouched by this merge), but its
    source row carries p="a" (an affected partition). A write pass that only
    sees the affected slice of the target would misread id=2 as source-only
    and insert a duplicate into partition a.
    """
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1, 2], "p": ["a", "b"], "v": ["x", "y"]}).write_deltalake(
        path, partition_cols=["p"]
    )
    b_files_before = {f for f in _data_files(path) if "p=b" in f}
    source = daft.from_pydict({"id": [1, 2, 3], "p": ["a", "a", "a"], "v": ["X", "y", "z"]})
    result = (
        daft.distributed_merge_deltalake(
            table=path,
            source=source,
            predicate="target.id = source.id",
            broadcast_join=broadcast,
        )
        .when_matched_update_all(predicate="source.v != target.v")
        .when_not_matched_insert_all()
        .execute()
    ).to_pydict()

    rows = _read_sorted(path)
    # id=2 appears exactly once, untouched, still in partition b.
    assert rows["id"] == [1, 2, 3]
    assert rows["p"] == ["a", "b", "a"]
    assert rows["v"] == ["X", "y", "z"]
    assert result["num_target_rows_inserted"][0] == 1
    assert result["num_target_rows_updated"][0] == 1
    assert {f for f in _data_files(path) if "p=b" in f} == b_files_before


def test_affected_partition_mixed_outcomes_with_untouched_partition(tmp_path):
    """Update + by-source delete + copy within one affected partition.

    The other partition is untouched and its files survive as-is.
    """
    path = str(tmp_path / "t")
    daft.from_pydict(
        {
            "id": [1, 2, 3, 4],
            "p": ["a", "a", "b", "a"],
            "flag": [False, True, False, False],
            "v": ["v1", "v2", "v3", "v4"],
        }
    ).write_deltalake(path, partition_cols=["p"])
    b_files_before = {f for f in _data_files(path) if "p=b" in f}
    source = daft.from_pydict({"id": [1], "p": ["a"], "flag": [False], "v": ["V1"]})
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_by_source_delete(predicate="target.flag = true")
        .execute()
    ).to_pydict()

    rows = _read_sorted(path)
    # id=1 updated, id=2 deleted (by-source, flag), id=3 untouched in b,
    # id=4 copied (by-source predicate false) and rewritten within a.
    assert rows["id"] == [1, 3, 4]
    assert rows["v"] == ["V1", "v3", "v4"]
    assert result["num_target_rows_updated"][0] == 1
    assert result["num_target_rows_deleted"][0] == 1
    assert {f for f in _data_files(path) if "p=b" in f} == b_files_before


@pytest.mark.parametrize("partitioned", [True, False])
def test_materialize_join_equivalence(tmp_path, partitioned):
    """materialize_join=True executes the join once; results are identical."""
    path = str(tmp_path / "t")
    daft.from_pydict(
        {"id": [1, 2, 3, 4], "p": ["a", "a", "b", "c"], "v": ["w", "x", "y", "z"]}
    ).write_deltalake(path, partition_cols=["p"] if partitioned else None)
    source = daft.from_pydict({"id": [1, 3, 5], "p": ["a", "b", "a"], "v": ["W", "Y", "new"]})
    result = (
        daft.distributed_merge_deltalake(
            table=path,
            source=source,
            predicate="target.id = source.id",
            materialize_join=True,
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    ).to_pydict()

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 3, 4, 5]
    assert rows["v"] == ["W", "x", "Y", "z", "new"]
    assert result["num_target_rows_updated"][0] == 2
    assert result["num_target_rows_inserted"][0] == 1
    assert result["num_target_rows_copied"][0] == (1 if partitioned else 2)


def test_insert_only_many_new_partitions(tmp_path):
    path = str(tmp_path / "t")
    daft.from_pydict({"id": [1, 2], "p": ["p0", "p0"], "v": ["a", "b"]}).write_deltalake(
        path, partition_cols=["p"]
    )
    p0_files_before = _data_files(path)

    source = daft.from_pydict(
        {"id": [10, 11, 12, 13, 14], "p": ["p1", "p2", "p3", "p4", "p5"], "v": list("vwxyz")}
    )
    result = (
        daft.distributed_merge_deltalake(table=path, source=source, predicate="target.id = source.id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    ).to_pydict()

    rows = _read_sorted(path)
    assert rows["id"] == [1, 2, 10, 11, 12, 13, 14]
    assert result["num_target_rows_inserted"][0] == 5
    assert result["num_target_rows_copied"][0] == 0
    # p0 was never touched: its files survive the merge byte-for-byte.
    assert p0_files_before <= _data_files(path)
