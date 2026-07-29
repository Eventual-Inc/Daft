from __future__ import annotations

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

import daft
from daft.io.iceberg._changelog_pairing import pair_updates, resolve_identifier_columns
from tests.conftest import get_tests_daft_runner_name


def _sorted_rows(df: daft.DataFrame) -> list[dict]:
    rows = df.to_pylist()
    return sorted(rows, key=lambda r: tuple(sorted(r.items(), key=lambda kv: kv[0])))


# --- Sequence pairing ---


def test_two_update_pairs_within_and_across_commits():
    # D3, I3, D4, I4 -- two independent UPDATE pairs for the same identifier.
    df = daft.from_pydict(
        {
            "id": [1, 1, 1, 1],
            "name": ["old3", "new3", "old4", "new4"],
            "_change_type": ["DELETE", "INSERT", "DELETE", "INSERT"],
            "_change_ordinal": [3, 3, 4, 4],
            "_commit_snapshot_id": [100, 100, 101, 101],
        }
    )
    result = _sorted_rows(pair_updates(df, ["id"]))
    by_name = {r["name"]: r["_change_type"] for r in result}
    assert by_name == {
        "old3": "UPDATE_BEFORE",
        "new3": "UPDATE_AFTER",
        "old4": "UPDATE_BEFORE",
        "new4": "UPDATE_AFTER",
    }


def test_insert_then_delete_does_not_reverse_pair():
    # I3, D4 -- an INSERT followed by a DELETE must not be paired (only DELETE-then-INSERT
    # pairs); both events stay independent.
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": ["a", "a"],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [3, 4],
            "_commit_snapshot_id": [100, 101],
        }
    )
    result = _sorted_rows(pair_updates(df, ["id"]))
    assert {r["_change_type"] for r in result} == {"INSERT", "DELETE"}


def test_cross_ordinal_pairing_is_deliberate():
    # D3, I7 with no intervening event for the identifier -- paired despite the gap,
    # matching Iceberg's ComputeUpdateIterator.
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": ["old", "new"],
            "_change_type": ["DELETE", "INSERT"],
            "_change_ordinal": [3, 7],
            "_commit_snapshot_id": [100, 104],
        }
    )
    result = _sorted_rows(pair_updates(df, ["id"]))
    by_name = {r["name"]: r["_change_type"] for r in result}
    assert by_name == {"old": "UPDATE_BEFORE", "new": "UPDATE_AFTER"}


def test_consecutive_delete_raises():
    # D3, D4 with no intervening INSERT -- identifier_columns can't be unique.
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": ["a", "a"],
            "_change_type": ["DELETE", "DELETE"],
            "_change_ordinal": [3, 4],
            "_commit_snapshot_id": [100, 101],
        }
    )
    with pytest.raises(Exception, match="consecutive DELETE"):
        pair_updates(df, ["id"]).collect()


def test_consecutive_insert_raises():
    # I3, I4 with no intervening DELETE.
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": ["a", "b"],
            "_change_type": ["INSERT", "INSERT"],
            "_change_ordinal": [3, 4],
            "_commit_snapshot_id": [100, 101],
        }
    )
    with pytest.raises(Exception, match="consecutive INSERT"):
        pair_updates(df, ["id"]).collect()


def test_cardinality_violation_two_deletes_same_ordinal_raises():
    df = daft.from_pydict(
        {
            "id": [1, 1, 1],
            "name": ["a", "b", "c"],
            "_change_type": ["DELETE", "DELETE", "INSERT"],
            "_change_ordinal": [3, 3, 4],
            "_commit_snapshot_id": [100, 100, 101],
        }
    )
    with pytest.raises(Exception, match="at most one DELETE and one INSERT"):
        pair_updates(df, ["id"]).collect()


def test_unknown_change_type_raises():
    df = daft.from_pydict(
        {
            "id": [1, 2],
            "name": ["a", "b"],
            "_change_type": ["INSERT", "GARBAGE"],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    with pytest.raises(Exception, match="unexpected _change_type value"):
        pair_updates(df, ["id"]).collect()


def test_null_change_type_raises():
    df = daft.from_pydict(
        {
            "id": [1, 2],
            "name": ["a", "b"],
            "_change_type": ["INSERT", None],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    with pytest.raises(Exception, match="unexpected _change_type value: None"):
        pair_updates(df, ["id"]).collect()


def test_composite_identifier_pairing():
    # Two identifier columns together identify a logical row; a change to either alone
    # must not be conflated with a different (tenant, id) pair.
    df = daft.from_pydict(
        {
            "tenant": ["t1", "t1", "t2", "t2"],
            "id": [1, 1, 1, 1],
            "name": ["old", "new", "other_old", "other_new"],
            "_change_type": ["DELETE", "INSERT", "DELETE", "INSERT"],
            "_change_ordinal": [3, 4, 3, 4],
            "_commit_snapshot_id": [100, 101, 100, 101],
        }
    )
    result = _sorted_rows(pair_updates(df, ["tenant", "id"]))
    by_name = {r["name"]: r["_change_type"] for r in result}
    assert by_name == {
        "old": "UPDATE_BEFORE",
        "new": "UPDATE_AFTER",
        "other_old": "UPDATE_BEFORE",
        "other_new": "UPDATE_AFTER",
    }


def test_pair_updates_rejects_empty_identifier_columns():
    # pair_updates() is a reusable module-level entry point and must not rely solely on
    # an upstream caller (resolve_identifier_columns) to keep this precondition.
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": ["old", "new"],
            "_change_type": ["DELETE", "INSERT"],
            "_change_ordinal": [3, 4],
            "_commit_snapshot_id": [100, 101],
        }
    )
    with pytest.raises(ValueError, match="non-empty identifier_columns"):
        pair_updates(df, [])


def test_same_ordinal_delete_before_insert_tie_break():
    """Fixes the DELETE-before-INSERT ordering within one ordinal as a DataFrame-level assertion.

    Same-ordinal DELETE+INSERT for the same identifier (the COW carryover shape) must
    resolve deterministically: DELETE always sorts before INSERT within an ordinal, so this
    pairs into UPDATE_BEFORE/UPDATE_AFTER rather than leaving the relative order undefined.
    """
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": ["old", "new"],
            "_change_type": ["DELETE", "INSERT"],
            "_change_ordinal": [3, 3],
            "_commit_snapshot_id": [100, 100],
        }
    )
    result = _sorted_rows(pair_updates(df, ["id"]))
    by_name = {r["name"]: r["_change_type"] for r in result}
    assert by_name == {"old": "UPDATE_BEFORE", "new": "UPDATE_AFTER"}

    # Also verify physical input row order (INSERT listed first, DELETE second) doesn't
    # change the outcome -- the type_rank-based sort, not physical arrival order, decides.
    reordered = daft.from_pydict(
        {
            "id": [1, 1],
            "name": ["new", "old"],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [3, 3],
            "_commit_snapshot_id": [100, 100],
        }
    )
    reordered_result = _sorted_rows(pair_updates(reordered, ["id"]))
    assert {r["name"]: r["_change_type"] for r in reordered_result} == by_name


def test_null_identifier_values_are_grouped_together():
    # Two rows with a NULL identifier must still be recognized as the same group and
    # paired, consistent with the NULL grouping semantics remove_carryovers already uses.
    df = daft.from_pydict(
        {
            "id": [None, None],
            "name": ["old", "new"],
            "_change_type": ["DELETE", "INSERT"],
            "_change_ordinal": [3, 4],
            "_commit_snapshot_id": [100, 101],
        }
    )
    result = _sorted_rows(pair_updates(df, ["id"]))
    by_name = {r["name"]: r["_change_type"] for r in result}
    assert by_name == {"old": "UPDATE_BEFORE", "new": "UPDATE_AFTER"}


def test_multi_partition_matches_single_partition_result():
    data = {
        "id": [1, 1, 1, 1, 2, 2],
        "name": ["old3", "new3", "old4", "new4", "x", "y"],
        "_change_type": ["DELETE", "INSERT", "DELETE", "INSERT", "DELETE", "INSERT"],
        "_change_ordinal": [3, 3, 4, 4, 0, 7],
        "_commit_snapshot_id": [100, 100, 101, 101, 50, 60],
    }
    single_partition_result = _sorted_rows(pair_updates(daft.from_pydict(data), ["id"]))

    multi_partition_df = daft.from_pydict(data).into_partitions(4)
    if get_tests_daft_runner_name() == "ray":
        assert multi_partition_df.num_partitions() == 4
    multi_partition_result = _sorted_rows(pair_updates(multi_partition_df, ["id"]))

    assert multi_partition_result == single_partition_result
    by_name = {r["name"]: r["_change_type"] for r in multi_partition_result}
    assert by_name == {
        "old3": "UPDATE_BEFORE",
        "new3": "UPDATE_AFTER",
        "old4": "UPDATE_BEFORE",
        "new4": "UPDATE_AFTER",
        "x": "UPDATE_BEFORE",
        "y": "UPDATE_AFTER",
    }


# --- Projection-pruning contract: validate only if consumed ---


def _cardinality_violating_df() -> daft.DataFrame:
    return daft.from_pydict(
        {
            "id": [1, 1, 1],
            "name": ["a", "b", "c"],
            "_change_type": ["DELETE", "DELETE", "INSERT"],
            "_change_ordinal": [3, 3, 4],
            "_commit_snapshot_id": [100, 100, 101],
        }
    )


def test_selecting_away_change_type_skips_validation():
    paired = pair_updates(_cardinality_violating_df(), ["id"])
    # Selecting only columns that don't depend on the rewritten _change_type must not
    # trigger the cardinality error -- the other columns' values are correct regardless of
    # whether the (skipped) validation would have passed.
    result = paired.select("id", "name").collect()
    assert sorted(row["name"] for row in result.to_pylist()) == ["a", "b", "c"]


def test_keeping_change_type_still_raises():
    paired = pair_updates(_cardinality_violating_df(), ["id"])
    with pytest.raises(Exception, match="at most one DELETE and one INSERT"):
        paired.select("id", "name", "_change_type").collect()


def test_no_select_at_all_still_raises():
    paired = pair_updates(_cardinality_violating_df(), ["id"])
    with pytest.raises(Exception, match="at most one DELETE and one INSERT"):
        paired.collect()


# --- resolve_identifier_columns validation ---


def _schema_with_identifier() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
        identifier_field_ids=[1],
    )


def _schema_without_identifier() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )


def test_default_none_resolves_to_schema_identifier_fields():
    assert resolve_identifier_columns(None, _schema_with_identifier()) == ["id"]


def test_default_none_with_no_schema_identifier_raises():
    with pytest.raises(ValueError, match="requires identifier_columns"):
        resolve_identifier_columns(None, _schema_without_identifier())


def test_explicit_empty_list_raises_distinctly_from_none():
    # Explicit [] must not be silently coerced to the schema default via truthiness.
    with pytest.raises(ValueError, match="may not be an empty list"):
        resolve_identifier_columns([], _schema_with_identifier())


def test_nonexistent_column_raises():
    with pytest.raises(ValueError, match="does not exist"):
        resolve_identifier_columns(["nonexistent"], _schema_with_identifier())


def test_duplicate_column_raises():
    with pytest.raises(ValueError, match="duplicate"):
        resolve_identifier_columns(["id", "id"], _schema_with_identifier())


def test_reserved_column_name_raises():
    with pytest.raises(ValueError, match="reserved changelog metadata column"):
        resolve_identifier_columns(["_change_type"], _schema_with_identifier())


def test_nested_path_raises():
    with pytest.raises(ValueError, match="nested"):
        resolve_identifier_columns(["struct.field"], _schema_with_identifier())


def test_explicit_valid_columns_are_accepted_as_is():
    assert resolve_identifier_columns(["data"], _schema_with_identifier()) == ["data"]
