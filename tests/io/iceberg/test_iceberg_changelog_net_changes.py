from __future__ import annotations

import math

import pyarrow as pa
import pytest

import daft
from daft.io.iceberg._changelog_net_changes import net_changes
from tests.conftest import get_tests_daft_runner_name


def _sorted_rows(df: daft.DataFrame) -> list[dict]:
    rows = df.to_pylist()
    return sorted(rows, key=lambda r: tuple(sorted(r.items(), key=lambda kv: kv[0])))


def _df(change_types: list[str], ordinals: list[int], commit_ids: list[int], grp: str = "g") -> daft.DataFrame:
    n = len(change_types)
    return daft.from_pydict(
        {
            "grp": [grp] * n,
            "_change_type": change_types,
            "_change_ordinal": ordinals,
            "_commit_snapshot_id": commit_ids,
        }
    )


# --- Core algorithm scenarios ---


def test_equal_counts_cancel_completely():
    df = _df(["INSERT", "INSERT", "DELETE", "DELETE"], [0, 1, 2, 3], [100, 101, 102, 103])
    assert _sorted_rows(net_changes(df)) == []


def test_alternating_survivor_is_last_event():
    df = _df(["INSERT", "DELETE", "INSERT", "DELETE", "INSERT"], [0, 1, 2, 3, 4], [100, 101, 102, 103, 104])
    result = _sorted_rows(net_changes(df))
    assert result == [
        {"grp": "g", "_change_type": "INSERT", "_change_ordinal": 4, "_commit_snapshot_id": 104},
    ]


def test_three_insert_one_delete_survives_as_two_with_first_ordinal_metadata():
    df = _df(["INSERT", "INSERT", "INSERT", "DELETE"], [0, 1, 2, 3], [100, 101, 102, 103])
    result = _sorted_rows(net_changes(df))
    assert result == [
        {"grp": "g", "_change_type": "INSERT", "_change_ordinal": 0, "_commit_snapshot_id": 100},
        {"grp": "g", "_change_type": "INSERT", "_change_ordinal": 0, "_commit_snapshot_id": 100},
    ]


def test_pure_delete_survivor_is_not_dropped_by_abs_fix():
    """Regression test for the range(n) negative-count bug: a lone DELETE must survive."""
    df = _df(["DELETE"], [0], [100])
    result = _sorted_rows(net_changes(df))
    assert result == [{"grp": "g", "_change_type": "DELETE", "_change_ordinal": 0, "_commit_snapshot_id": 100}]


def test_three_delete_one_insert_survives_as_two_deletes():
    # 3 DELETE - 1 INSERT = net -2 (mirrors the existing 3-INSERT-1-DELETE scenario).
    df = _df(["DELETE", "DELETE", "DELETE", "INSERT"], [0, 1, 2, 3], [100, 101, 102, 103])
    result = _sorted_rows(net_changes(df))
    assert result == [
        {"grp": "g", "_change_type": "DELETE", "_change_ordinal": 0, "_commit_snapshot_id": 100},
        {"grp": "g", "_change_type": "DELETE", "_change_ordinal": 0, "_commit_snapshot_id": 100},
    ]


def test_reset_then_new_segment_starting_with_delete():
    """A segment can validly start with DELETE after an earlier reset.

    Must not misfire the sign-consistency check.
    """
    # INSERT@0,DELETE@1 (cancels, running 1->0) ; DELETE@2,DELETE@3 (new segment starting
    # at DELETE, net -2, survives as two DELETEs with ordinal=2 metadata).
    df = _df(["INSERT", "DELETE", "DELETE", "DELETE"], [0, 1, 2, 3], [100, 101, 102, 103])
    result = _sorted_rows(net_changes(df))
    # First pair (INSERT@0, DELETE@1) cancels (running 1->0). New segment starts at
    # DELETE@2: running goes -1, -2 -- net -2, survives as two DELETEs with ordinal=2
    # metadata (the new segment's own first row), not ordinal=0/1's.
    assert result == [
        {"grp": "g", "_change_type": "DELETE", "_change_ordinal": 2, "_commit_snapshot_id": 102},
        {"grp": "g", "_change_type": "DELETE", "_change_ordinal": 2, "_commit_snapshot_id": 102},
    ]


def test_single_delete_event_length_one_segment():
    df = _df(["DELETE"], [5], [500])
    assert _sorted_rows(net_changes(df)) == [
        {"grp": "g", "_change_type": "DELETE", "_change_ordinal": 5, "_commit_snapshot_id": 500}
    ]


def test_single_insert_event_length_one_segment():
    df = _df(["INSERT"], [5], [500])
    assert _sorted_rows(net_changes(df)) == [
        {"grp": "g", "_change_type": "INSERT", "_change_ordinal": 5, "_commit_snapshot_id": 500}
    ]


def test_same_ordinal_mixed_types_triggers_mid_ordinal_reset():
    """Fixes the deterministic (_change_ordinal, type_rank) order key as a regression test.

    A prior INSERT brings the running total to 1; a same-ordinal (DELETE, INSERT) pair
    (DELETE sorts before INSERT within the ordinal) then genuinely resets mid-pair --
    this is faithful replication of Spark's own row-by-row semantics under its real
    sortSpec, not a bug.
    """
    df = _df(["INSERT", "DELETE", "INSERT"], [0, 1, 1], [100, 101, 101])
    result = _sorted_rows(net_changes(df))
    # The first pair (INSERT@0, DELETE@1) cancels; only the same-ordinal INSERT survives,
    # carrying its own (ordinal=1, commit=101) metadata -- not ordinal=0's.
    assert result == [{"grp": "g", "_change_type": "INSERT", "_change_ordinal": 1, "_commit_snapshot_id": 101}]


# --- Illegal _change_type values ---


def test_null_change_type_raises():
    df = _df(["INSERT", None], [0, 1], [100, 101])
    with pytest.raises(Exception, match="unexpected _change_type value: None"):
        net_changes(df).collect()


def test_update_before_change_type_raises():
    df = _df(["UPDATE_BEFORE"], [0], [100])
    with pytest.raises(Exception, match="UPDATE_BEFORE"):
        net_changes(df).collect()


def test_update_after_change_type_raises():
    df = _df(["UPDATE_AFTER"], [0], [100])
    with pytest.raises(Exception, match="UPDATE_AFTER"):
        net_changes(df).collect()


def test_unknown_change_type_string_raises():
    df = _df(["garbage"], [0], [100])
    with pytest.raises(Exception, match="garbage"):
        net_changes(df).collect()


# --- Sign-consistency validation dependency chain ---


def test_sign_check_runs_even_when_change_type_is_not_selected():
    """The symptom-of-a-real-bug sign check must not be skippable by selecting it away.

    Unlike compute_updates' cardinality check (which is only guaranteed when
    `_change_type` is consumed), net_changes' sign-consistency check sits on the
    `replay_count` -> `explode` dependency chain that determines *row count itself*, so it
    always runs regardless of the final `.select()`.
    """
    import daft.io.iceberg._changelog_net_changes as nc

    original = nc._resolve_net_change_type

    @daft.func(return_dtype=nc._NET_RESOLUTION_DTYPE, on_error="raise")
    def boom(net_count, seg_first_change_type):
        raise ValueError("sign check ran")

    nc._resolve_net_change_type = boom
    try:
        df = _df(["INSERT", "INSERT", "INSERT", "DELETE"], [0, 1, 2, 3], [100, 101, 102, 103])
        result = net_changes(df)
        with pytest.raises(Exception, match="sign check ran"):
            result.select("grp").collect()
    finally:
        nc._resolve_net_change_type = original


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="Call counting relies on process-local state; Ray executes UDFs in separate "
    "worker processes, so a shared Python counter can't observe every invocation.",
)
def test_resolve_net_change_type_called_once_per_surviving_segment():
    """`_resolve_net_change_type` must be called exactly once per surviving segment.

    Not once per output row (post-`explode`) and not twice per segment (once for
    `_change_type`, once for `replay_count`, if the two `.with_column()` extractions
    accidentally re-triggered the UDF instead of sharing one materialized struct column).
    """
    import daft.io.iceberg._changelog_net_changes as nc

    call_count = 0
    original = nc._resolve_net_change_type

    @daft.func(return_dtype=nc._NET_RESOLUTION_DTYPE, on_error="raise")
    def counting(net_count, seg_first_change_type):
        nonlocal call_count
        call_count += 1
        if net_count > 0:
            return {"change_type": "INSERT", "replay_count": net_count}
        return {"change_type": "DELETE", "replay_count": abs(net_count)}

    nc._resolve_net_change_type = counting
    try:
        # Three groups, each with exactly one surviving segment: g1 nets +2 (2 output
        # rows), g2 nets -1 (1 row), g3 nets -2 (2 rows) -- 5 output rows from 3 segments.
        df = daft.from_pydict(
            {
                "grp": ["g1"] * 4 + ["g2"] + ["g3"] * 4,
                "_change_type": ["INSERT", "INSERT", "INSERT", "DELETE"]
                + ["DELETE"]
                + ["DELETE", "DELETE", "DELETE", "INSERT"],
                "_change_ordinal": [0, 1, 2, 3] + [0] + [0, 1, 2, 3],
                "_commit_snapshot_id": [100, 101, 102, 103] + [200] + [300, 301, 302, 303],
            }
        )
        result = nc.net_changes(df).to_pylist()
        assert len(result) == 5
        assert call_count == 3
    finally:
        nc._resolve_net_change_type = original


# --- Equality semantics for the net_changes grouping key (mirrors remove_carryovers'
# equivalent tests in test_iceberg_changelog_postprocess.py -- net_changes groups by the
# same "every non-metadata column" key, so the same equality edge cases apply) ---


def test_null_valued_column_cancels():
    # A NULL in a data column must be treated as an ordinary comparable value for
    # grouping (SQL's NULL != NULL would otherwise prevent cancellation).
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": [None, None],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    assert _sorted_rows(net_changes(df)) == []


def test_null_valued_column_does_not_cancel_against_non_null():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "name": [None, "a"],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    assert len(_sorted_rows(net_changes(df))) == 2


def test_nan_valued_column_cancels():
    # NaN must be treated as an ordinary comparable value (bitwise/positional), not
    # IEEE-754 "NaN != NaN" -- otherwise NaN-containing rows could never net-cancel.
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "val": [math.nan, math.nan],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    assert _sorted_rows(net_changes(df)) == []


def test_nan_valued_column_does_not_cancel_against_non_nan():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "val": [math.nan, 1.5],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    assert len(_sorted_rows(net_changes(df))) == 2


def test_struct_column_cancels():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "info": [{"a": 1, "b": "x"}, {"a": 1, "b": "x"}],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    assert _sorted_rows(net_changes(df)) == []


def test_struct_column_does_not_cancel_when_field_differs():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "info": [{"a": 1, "b": "x"}, {"a": 2, "b": "x"}],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    assert len(_sorted_rows(net_changes(df))) == 2


def test_list_column_cancels():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "tags": [[1, 2, 3], [1, 2, 3]],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    assert _sorted_rows(net_changes(df)) == []


def test_list_column_does_not_cancel_when_order_differs():
    df = daft.from_pydict(
        {
            "id": [1, 1],
            "tags": [[1, 2, 3], [3, 2, 1]],
            "_change_type": ["INSERT", "DELETE"],
            "_change_ordinal": [0, 1],
            "_commit_snapshot_id": [100, 101],
        }
    )
    assert len(_sorted_rows(net_changes(df))) == 2


def test_map_column_cancels():
    map_type = pa.map_(pa.field("key", pa.string(), nullable=False), pa.field("value", pa.int64()))
    table = pa.table(
        {
            "id": pa.array([1, 1], type=pa.int64()),
            "kv": pa.array([[("k", 1)], [("k", 1)]], type=map_type),
            "_change_type": pa.array(["INSERT", "DELETE"]),
            "_change_ordinal": pa.array([0, 1], type=pa.int64()),
            "_commit_snapshot_id": pa.array([100, 101], type=pa.int64()),
        }
    )
    df = daft.from_arrow(table)
    assert _sorted_rows(net_changes(df)) == []


def test_map_column_does_not_cancel_when_value_differs():
    map_type = pa.map_(pa.field("key", pa.string(), nullable=False), pa.field("value", pa.int64()))
    table = pa.table(
        {
            "id": pa.array([1, 1], type=pa.int64()),
            "kv": pa.array([[("k", 1)], [("k", 2)]], type=map_type),
            "_change_type": pa.array(["INSERT", "DELETE"]),
            "_change_ordinal": pa.array([0, 1], type=pa.int64()),
            "_commit_snapshot_id": pa.array([100, 101], type=pa.int64()),
        }
    )
    df = daft.from_arrow(table)
    assert len(_sorted_rows(net_changes(df))) == 2


# --- Grouping isolation and multi-group stability ---


def test_different_data_content_rows_do_not_interfere():
    df = daft.from_pydict(
        {
            "grp": ["a", "a", "b"],
            "_change_type": ["INSERT", "DELETE", "INSERT"],
            "_change_ordinal": [0, 1, 0],
            "_commit_snapshot_id": [100, 101, 100],
        }
    )
    result = _sorted_rows(net_changes(df))
    assert result == [{"grp": "b", "_change_type": "INSERT", "_change_ordinal": 0, "_commit_snapshot_id": 100}]


def test_multiple_groups_step3_first_last_stable():
    df = daft.from_pydict(
        {
            "grp": ["g1"] * 5 + ["g2"] * 4 + ["g3"] * 4,
            "_change_type": ["INSERT", "DELETE", "INSERT", "DELETE", "INSERT"]
            + ["INSERT", "INSERT", "DELETE", "DELETE"]
            + ["INSERT", "INSERT", "INSERT", "DELETE"],
            "_change_ordinal": [0, 1, 2, 3, 4] + [0, 1, 2, 3] + [0, 1, 2, 3],
            "_commit_snapshot_id": [100, 101, 102, 103, 104] + [200, 201, 202, 203] + [300, 301, 302, 303],
        }
    )
    rows = net_changes(df).to_pylist()
    # g1 (alternating, ends on INSERT) survives once at its last ordinal; g2 (2 INSERT + 2
    # DELETE, net 0) fully cancels; g3 (3 INSERT + 1 DELETE) survives twice with its first
    # ordinal's metadata.
    assert sorted((r["grp"], r["_change_type"], r["_change_ordinal"], r["_commit_snapshot_id"]) for r in rows) == [
        ("g1", "INSERT", 4, 104),
        ("g3", "INSERT", 0, 300),
        ("g3", "INSERT", 0, 300),
    ]


def test_multi_partition_matches_single_partition_result():
    data = {
        "grp": ["g1"] * 5 + ["g2"] * 4 + ["g3"] * 4,
        "_change_type": ["INSERT", "DELETE", "INSERT", "DELETE", "INSERT"]
        + ["INSERT", "INSERT", "DELETE", "DELETE"]
        + ["INSERT", "INSERT", "INSERT", "DELETE"],
        "_change_ordinal": [0, 1, 2, 3, 4] + [0, 1, 2, 3] + [0, 1, 2, 3],
        "_commit_snapshot_id": [100, 101, 102, 103, 104] + [200, 201, 202, 203] + [300, 301, 302, 303],
    }
    single_partition_result = _sorted_rows(net_changes(daft.from_pydict(data)))

    multi_partition_df = daft.from_pydict(data).into_partitions(4)
    if get_tests_daft_runner_name() == "ray":
        assert multi_partition_df.num_partitions() == 4
    multi_partition_result = _sorted_rows(net_changes(multi_partition_df))

    assert multi_partition_result == single_partition_result


# --- Internal column naming must not collide with user data columns ---


def test_business_columns_named_like_first_choice_internal_names_are_preserved():
    """A user column literally named like the generator's *first-choice* candidate must survive.

    Uses the actual first-choice names `_fresh_internal_names` produces for this module
    (`__daft_net_changes_running` etc.), not arbitrary business names, so this genuinely
    forces a fallback to the `_1`-suffixed candidates -- unlike names that don't collide at
    all, which wouldn't exercise the fallback path regardless of whether collision-safety
    was implemented correctly.
    """
    df = daft.from_pydict(
        {
            "grp": ["g", "g", "g"],
            "__daft_net_changes_running": ["x", "x", "y"],
            "__daft_net_changes_run_id": [1, 1, 2],
            "__daft_net_changes_signed": ["a", "a", "b"],
            "__daft_net_changes_rn": [True, True, False],
            "_change_type": ["INSERT", "DELETE", "INSERT"],
            "_change_ordinal": [0, 1, 0],
            "_commit_snapshot_id": [100, 101, 100],
        }
    )
    # Rows 0/1 are byte-identical on every business column (same data group) and cancel
    # (INSERT+DELETE, net 0); row 2 has distinct business-column values, so it's a separate
    # group and survives standalone -- this exercises both "collision-safe naming" and
    # "cancellation is still computed correctly under it" at once.
    result = net_changes(df).to_pylist()
    assert result == [
        {
            "grp": "g",
            "__daft_net_changes_running": "y",
            "__daft_net_changes_run_id": 2,
            "__daft_net_changes_signed": "b",
            "__daft_net_changes_rn": False,
            "_change_type": "INSERT",
            "_change_ordinal": 0,
            "_commit_snapshot_id": 100,
        }
    ]


def test_business_columns_occupying_first_and_second_choice_names_force_third_round():
    """Business columns squatting on *both* the first- and `_1`-suffixed candidate names.

    Forces `_fresh_internal_names` past its first retry into a second one (`_2`).
    """
    df = daft.from_pydict(
        {
            "grp": ["g", "g", "g"],
            "__daft_net_changes_running": ["x", "x", "y"],
            "__daft_net_changes_running_1": ["p", "p", "q"],
            "_change_type": ["INSERT", "DELETE", "INSERT"],
            "_change_ordinal": [0, 1, 0],
            "_commit_snapshot_id": [100, 101, 100],
        }
    )
    result = net_changes(df).to_pylist()
    assert result == [
        {
            "grp": "g",
            "__daft_net_changes_running": "y",
            "__daft_net_changes_running_1": "q",
            "_change_type": "INSERT",
            "_change_ordinal": 0,
            "_commit_snapshot_id": 100,
        }
    ]


def test_business_columns_named_like_internal_names_multi_partition():
    """The first-choice collision scenario must also hold under real multi-partition execution."""
    data = {
        "grp": ["g", "g", "g"],
        "__daft_net_changes_running": ["x", "x", "y"],
        "__daft_net_changes_run_id": [1, 1, 2],
        "_change_type": ["INSERT", "DELETE", "INSERT"],
        "_change_ordinal": [0, 1, 0],
        "_commit_snapshot_id": [100, 101, 100],
    }
    df = daft.from_pydict(data).into_partitions(4)
    if get_tests_daft_runner_name() == "ray":
        assert df.num_partitions() == 4
    result = net_changes(df).to_pylist()
    assert result == [
        {
            "grp": "g",
            "__daft_net_changes_running": "y",
            "__daft_net_changes_run_id": 2,
            "_change_type": "INSERT",
            "_change_ordinal": 0,
            "_commit_snapshot_id": 100,
        }
    ]


# --- Large net_count correctness test (not a benchmark) ---


def test_large_net_count_expands_correctly():
    n = 5000
    change_types = ["INSERT"] * n + ["DELETE"]
    ordinals = list(range(n + 1))
    commit_ids = list(range(100, 100 + n + 1))
    df = _df(change_types, ordinals, commit_ids)
    result = net_changes(df).to_pylist()
    assert len(result) == n - 1
    assert all(r["_change_type"] == "INSERT" for r in result)
    assert all(r["_change_ordinal"] == 0 for r in result)
