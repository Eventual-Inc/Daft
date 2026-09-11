from __future__ import annotations

import math

import pyarrow as pa

import daft
from daft.expressions import col
from daft.io.iceberg._changelog_postprocess import remove_carryovers
from tests.conftest import get_tests_daft_runner_name


def _sorted_rows(df: daft.DataFrame) -> list[dict]:
    rows = df.to_pylist()
    return sorted(rows, key=lambda r: tuple(sorted(r.items(), key=lambda kv: kv[0])))


def test_three_insert_two_delete_leaves_one_insert():
    df = daft.from_pydict(
        {
            "id": [1, 1, 1, 1, 1],
            "name": ["a", "a", "a", "a", "a"],
            "_change_type": ["INSERT", "INSERT", "INSERT", "DELETE", "DELETE"],
            "_change_ordinal": [0, 0, 0, 0, 0],
            "_commit_snapshot_id": [100, 100, 100, 100, 100],
        }
    )
    result = _sorted_rows(remove_carryovers(df))
    assert result == [
        {"id": 1, "name": "a", "_change_type": "INSERT", "_change_ordinal": 0, "_commit_snapshot_id": 100}
    ]


def test_two_insert_three_delete_leaves_one_delete():
    df = daft.from_pydict(
        {
            "id": [1, 1, 1, 1, 1],
            "name": ["a", "a", "a", "a", "a"],
            "_change_type": ["INSERT", "INSERT", "DELETE", "DELETE", "DELETE"],
            "_change_ordinal": [0, 0, 0, 0, 0],
            "_commit_snapshot_id": [100, 100, 100, 100, 100],
        }
    )
    result = _sorted_rows(remove_carryovers(df))
    assert result == [
        {"id": 1, "name": "a", "_change_type": "DELETE", "_change_ordinal": 0, "_commit_snapshot_id": 100}
    ]


def test_equal_counts_cancel_completely():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": ["a", "a"],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    assert _sorted_rows(remove_carryovers(df)) == []


def test_unrelated_row_in_same_commit_is_untouched():
    df = daft.from_pydict(
        {
            "id": [1, 1, 1, 2],
            "name": ["a", "a", "a", "b"],
            "_change_type": ["INSERT", "INSERT", "DELETE", "DELETE"],
            "_change_ordinal": [0, 0, 0, 0],
            "_commit_snapshot_id": [100, 100, 100, 100],
        }
    )
    result = _sorted_rows(remove_carryovers(df))
    # "DELETE" sorts before "INSERT" lexicographically, which is what _sorted_rows's sort
    # key (over key-alphabetical (k, v) tuples, starting with _change_type) produces.
    assert result == [
        {"id": 2, "name": "b", "_change_type": "DELETE", "_change_ordinal": 0, "_commit_snapshot_id": 100},
        {"id": 1, "name": "a", "_change_type": "INSERT", "_change_ordinal": 0, "_commit_snapshot_id": 100},
    ]


def test_identical_content_across_different_commits_is_not_cancelled():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": ["a", "a"],
            "_change_type": ["DELETE", "INSERT"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    result = _sorted_rows(remove_carryovers(df))
    assert result == [
        {"id": 1, "name": "a", "_change_type": "DELETE", "_change_ordinal": 0, "_commit_snapshot_id": 100},
        {"id": 1, "name": "a", "_change_type": "INSERT", "_change_ordinal": 1, "_commit_snapshot_id": 101},
    ]


def test_pure_insert_no_delete_is_untouched():
    df = daft.from_pydict(
        {
            "id": [1, 2],
            "name": ["a", "b"],
            "_change_type": ["INSERT", "INSERT"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    assert _sorted_rows(remove_carryovers(df)) == _sorted_rows(df)


def test_result_independent_of_row_order():
    base = {
        "id": [1, 1, 1, 1, 1],
        "name": ["a", "a", "a", "a", "a"],
        "_change_type": ["DELETE", "INSERT", "INSERT", "INSERT", "DELETE"],
        "_change_ordinal": [0, 0, 0, 0, 0],
        "_commit_snapshot_id": [100, 100, 100, 100, 100],
    }
    reordered = {k: list(reversed(v)) for k, v in base.items()}
    assert _sorted_rows(remove_carryovers(daft.from_pydict(base))) == _sorted_rows(
        remove_carryovers(daft.from_pydict(reordered))
    )


# --- Equality semantics for the carryover grouping key ---


def test_null_valued_column_carryover_is_cancelled():
    # Two rows identical on every column, including a NULL in a data column: must be
    # recognized as the same equality group and cancelled, not treated as
    # non-comparable (SQL's NULL != NULL would otherwise prevent cancellation).
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": [None, None],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    assert _sorted_rows(remove_carryovers(df)) == []


def test_null_valued_column_does_not_cancel_against_non_null():
    # A NULL-valued row and a non-NULL row for the same id are NOT carryover noise --
    # they're different content and must both survive.
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": [None, "a"],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    assert len(_sorted_rows(remove_carryovers(df))) == 2


def test_nan_valued_column_carryover_is_cancelled():
    # Two rows identical including a NaN float column: NaN must be treated as an
    # ordinary comparable value for grouping purposes (bitwise/positional comparison,
    # not IEEE-754 "NaN != NaN"), otherwise NaN-containing carryover rows could never be
    # recognized as identical and would leak into the output.
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "val": [math.nan, math.nan],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    assert _sorted_rows(remove_carryovers(df)) == []


def test_struct_column_carryover_is_cancelled():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "info": [{"a": 1, "b": "x"}, {"a": 1, "b": "x"}],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    assert _sorted_rows(remove_carryovers(df)) == []


def test_struct_column_does_not_cancel_when_field_differs():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "info": [{"a": 1, "b": "x"}, {"a": 2, "b": "x"}],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    assert len(_sorted_rows(remove_carryovers(df))) == 2


def test_list_column_carryover_is_cancelled():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "tags": [[1, 2, 3], [1, 2, 3]],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    assert _sorted_rows(remove_carryovers(df)) == []


def test_list_column_does_not_cancel_when_order_differs():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "tags": [[1, 2, 3], [3, 2, 1]],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 0],
            "_commit_snapshot_id": [100, 100],
        }
    )
    assert len(_sorted_rows(remove_carryovers(df))) == 2


def test_map_column_carryover_is_cancelled():
    map_type = pa.map_(pa.field("key", pa.string(), nullable=False), pa.field("value", pa.int64()))
    table = pa.table(
        {
            "id": pa.array([1, 1], type=pa.int64()),
            "kv": pa.array([[("k", 1)], [("k", 1)]], type=map_type),
            "_change_type": pa.array(["INSERT", "DELETE"]),
            "_change_ordinal": pa.array([0, 0], type=pa.int64()),
            "_commit_snapshot_id": pa.array([100, 100], type=pa.int64()),
        }
    )
    df = daft.from_arrow(table)
    assert _sorted_rows(remove_carryovers(df)) == []


# --- Multi-partition execution consistency ---


def test_multi_partition_carryover_matches_single_partition_result():
    """Multi-partition carryover cancellation must match the single-partition result.

    `into_partitions`/`repartition` are no-ops under the native runner (single partition
    always), so this test is only a genuine multi-partition exercise under DAFT_RUNNER=ray;
    it remains a harmless single-partition re-check under native.
    """
    data = {
        "id": [1, 1, 1, 1, 1, 2, 3, 3],
        "name": ["a", "a", "a", "a", "a", "b", "c", "c"],
        "_change_type": ["INSERT", "INSERT", "INSERT", "DELETE", "DELETE", "DELETE", "INSERT", "DELETE"],
        "_change_ordinal": [0, 0, 0, 0, 0, 0, 1, 1],
        "_commit_snapshot_id": [100, 100, 100, 100, 100, 100, 101, 101],
    }
    single_partition_result = _sorted_rows(remove_carryovers(daft.from_pydict(data)))

    multi_partition_df = daft.from_pydict(data).into_partitions(4)
    if get_tests_daft_runner_name() == "ray":
        assert multi_partition_df.num_partitions() == 4
    multi_partition_result = _sorted_rows(remove_carryovers(multi_partition_df))

    assert multi_partition_result == single_partition_result
    # 3 INSERT/2 DELETE of (1,a) -> 1 INSERT; unrelated (2,b) DELETE untouched; (3,c)
    # 1 INSERT/1 DELETE -> fully cancelled. "DELETE" sorts before "INSERT"
    # lexicographically, matching _sorted_rows's key order.
    assert multi_partition_result == [
        {"id": 2, "name": "b", "_change_type": "DELETE", "_change_ordinal": 0, "_commit_snapshot_id": 100},
        {"id": 1, "name": "a", "_change_type": "INSERT", "_change_ordinal": 0, "_commit_snapshot_id": 100},
    ]


# --- Internal column naming must not collide with user data columns ---


def test_fresh_internal_names_avoids_collisions():
    """Unit test for the name generator itself: returned names must never be in `existing`.

    Covers no collision (first choice used directly), a first-choice collision (falls back
    to the `_1` suffix), and a first-choice-and-`_1` collision (falls back to `_2`).
    """
    from daft.io.iceberg._changelog_postprocess import _fresh_internal_names

    basenames = ("occurrence", "n_insert", "n_delete")

    names = _fresh_internal_names(set(), prefix="carryover", basenames=basenames)
    assert set(names.values()) == {
        "__daft_carryover_occurrence",
        "__daft_carryover_n_insert",
        "__daft_carryover_n_delete",
    }

    first_choice_taken = {
        "__daft_carryover_occurrence",
        "__daft_carryover_n_insert",
        "__daft_carryover_n_delete",
    }
    names = _fresh_internal_names(first_choice_taken, prefix="carryover", basenames=basenames)
    assert not (set(names.values()) & first_choice_taken)
    assert set(names.values()) == {
        "__daft_carryover_occurrence_1",
        "__daft_carryover_n_insert_1",
        "__daft_carryover_n_delete_1",
    }

    first_and_second_taken = first_choice_taken | {
        "__daft_carryover_occurrence_1",
        "__daft_carryover_n_insert_1",
        "__daft_carryover_n_delete_1",
    }
    names = _fresh_internal_names(first_and_second_taken, prefix="carryover", basenames=basenames)
    assert not (set(names.values()) & first_and_second_taken)


def test_business_columns_named_like_first_choice_internal_names_are_preserved():
    """A user column literally named like the generator's *first-choice* candidate must survive.

    `remove_carryovers` used to `.with_column()` plain names like `_carryover_occurrence`
    and then `.exclude()` them -- if a user's table happened to have a column with that
    exact name, its original values would be silently overwritten and then deleted. This
    test uses the actual first-choice candidate names the *current* generator produces
    (`__daft_carryover_occurrence` etc.), not the pre-fix literal names, so it genuinely
    forces `_fresh_internal_names` to fall back to its `_1`-suffixed second choice and
    verifies that fallback path end to end -- a test using unrelated business names
    wouldn't exercise this collision branch at all, regardless of implementation.
    """
    df = daft.from_pydict(
        {
            "id": [1, 1, 2],
            "__daft_carryover_occurrence": ["biz_a", "biz_a", "biz_c"],
            "__daft_carryover_n_insert": [10, 10, 30],
            "__daft_carryover_n_delete": [40, 40, 60],
            "_change_type": ["INSERT", "DELETE", "INSERT"],
            "_change_ordinal": [0, 0, 0],
            "_commit_snapshot_id": [100, 100, 100],
        }
    )
    result = _sorted_rows(remove_carryovers(df))
    # id=1's INSERT/DELETE pair is byte-identical on every data column (including the three
    # business columns above, which are just regular data columns as far as
    # remove_carryovers is concerned) plus _change_ordinal and _commit_snapshot_id, so it's
    # genuine carryover noise and cancels; only id=2's standalone INSERT survives, and its
    # business column values -- and the output schema's column names -- must be untouched.
    assert result == [
        {
            "id": 2,
            "__daft_carryover_occurrence": "biz_c",
            "__daft_carryover_n_insert": 30,
            "__daft_carryover_n_delete": 60,
            "_change_type": "INSERT",
            "_change_ordinal": 0,
            "_commit_snapshot_id": 100,
        }
    ]


def test_business_columns_occupying_first_and_second_choice_names_force_third_round():
    """Business columns squatting on *both* the first- and `_1`-suffixed candidate names.

    Forces `_fresh_internal_names` past its first retry into a second one (`_2`), and
    verifies all six business columns -- not just three -- keep their original values and
    remain in the output schema.
    """
    df = daft.from_pydict(
        {
            "id": [1, 1, 2],
            "__daft_carryover_occurrence": ["r0_a", "r0_a", "r0_c"],
            "__daft_carryover_n_insert": [10, 10, 30],
            "__daft_carryover_n_delete": [40, 40, 60],
            "__daft_carryover_occurrence_1": ["r1_a", "r1_a", "r1_c"],
            "__daft_carryover_n_insert_1": [11, 11, 31],
            "__daft_carryover_n_delete_1": [41, 41, 61],
            "_change_type": ["INSERT", "DELETE", "INSERT"],
            "_change_ordinal": [0, 0, 0],
            "_commit_snapshot_id": [100, 100, 100],
        }
    )
    result = _sorted_rows(remove_carryovers(df))
    assert result == [
        {
            "id": 2,
            "__daft_carryover_occurrence": "r0_c",
            "__daft_carryover_n_insert": 30,
            "__daft_carryover_n_delete": 60,
            "__daft_carryover_occurrence_1": "r1_c",
            "__daft_carryover_n_insert_1": 31,
            "__daft_carryover_n_delete_1": 61,
            "_change_type": "INSERT",
            "_change_ordinal": 0,
            "_commit_snapshot_id": 100,
        }
    ]


# --- Optimizer boundary: filter must not cross the carryover window ---


def test_filter_after_remove_carryovers_does_not_resurrect_cancelled_pairs():
    """A cancelled carryover pair must not be resurrected by a filter applied afterward.

    A carryover pair (byte-identical INSERT+DELETE) must still cancel completely even
    when the caller applies a `.where(col("_change_type") == "INSERT")` filter on the
    *result* of `remove_carryovers`. If the optimizer ever pushed such a filter down
    across the carryover window (into the scan, before pairing), it would silently keep
    the INSERT half of a carryover pair that should have cancelled -- the
    "one side of the pair survives" failure mode.
    """
    df = daft.from_pydict(
        {
            "id": [1, 1, 2],
            "name": ["a", "a", "b"],
            "_change_type": ["INSERT", "DELETE", "INSERT"],
            "_change_ordinal": [0, 0, 0],
            "_commit_snapshot_id": [100, 100, 100],
        }
    )
    resolved = remove_carryovers(df)
    only_inserts = resolved.where(col("_change_type") == "INSERT")
    # id=1's INSERT/DELETE pair is genuine carryover noise and must already be gone from
    # `resolved`; filtering to INSERT afterward must not resurrect it. Only id=2's
    # standalone INSERT should remain.
    assert _sorted_rows(only_inserts) == [
        {"id": 2, "name": "b", "_change_type": "INSERT", "_change_ordinal": 0, "_commit_snapshot_id": 100}
    ]
