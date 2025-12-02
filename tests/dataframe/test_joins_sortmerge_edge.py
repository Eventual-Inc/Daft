from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataType

# Run these edge cases only under sort_merge strategy
JOIN_STRATEGY = "sort_merge"


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize("how", ["semi", "anti"])
def test_sort_merge_right_all_empty_partitions(how, repartition_nparts, make_df, with_morsel_size):
    # After range repartition on the right by key, all partitions are empty: semi should be empty; anti should equal the left
    left = make_df(
        {"id": [1, 2, 3, None], "lv": ["a", "b", "c", "d"]}, repartition=repartition_nparts, repartition_columns=["id"]
    )
    right = make_df({"id": [], "rv": []}, repartition=repartition_nparts, repartition_columns=["id"])

    out = (
        left.join(right, left_on=["id"], right_on=["id"], how=how, strategy=JOIN_STRATEGY)
        .sort(["id", "lv"])
        .select("id", "lv")
    )

    expected = {"semi": {"id": [], "lv": []}, "anti": {"id": [1, 2, 3, None], "lv": ["a", "b", "c", "d"]}}[how]
    assert (
        pa.Table.from_pydict(out.to_pydict()).sort_by([("id", "ascending")]).to_pydict()
        == pa.Table.from_pydict(expected).sort_by([("id", "ascending")]).to_pydict()
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize("nulls_first", [True, False])
@pytest.mark.parametrize("descending", [True, False])
@pytest.mark.parametrize("how", ["semi", "anti"])
def test_sort_merge_multikey_with_nulls(how, repartition_nparts, nulls_first, descending, make_df, with_morsel_size):
    # Multi-key with nulls: only non-null exact equality participates in semi; anti retains left rows with no match
    # Left
    left = make_df(
        {
            "k1": [1, 2, None, 3],
            "k2": ["a", "b", "x", None],
            "lv": ["L1", "L2", "L3", "L4"],
        },
        repartition=repartition_nparts,
        repartition_columns=["k1"],
    )
    # Right
    right = make_df(
        {
            "k1": [2, 3, None],
            "k2": ["b", None, "x"],
            "rv": ["R2", "R4", "R3"],
        },
        repartition=repartition_nparts,
        repartition_columns=["k1"],
    )

    # Sort behavior is controlled by fixtures; defaults: nulls_first=True, descending=False
    out = (
        left.join(right, left_on=["k1", "k2"], right_on=["k1", "k2"], how=how, strategy=JOIN_STRATEGY)
        .sort(["k1", "k2", "lv"])
        .select("k1", "k2", "lv")
    )

    if how == "semi":
        expected = {"k1": [2], "k2": ["b"], "lv": ["L2"]}
    else:
        # Anti-join: include left rows with no exact equality match on all keys.
        # Rows with any null in keys never match under SQL equality semantics.
        expected = {"k1": [1, 3, None], "k2": ["a", None, "x"], "lv": ["L1", "L4", "L3"]}
    assert (
        pa.Table.from_pydict(out.to_pydict()).sort_by([("k1", "ascending"), ("k2", "ascending")]).to_pydict()
        == pa.Table.from_pydict(expected).sort_by([("k1", "ascending"), ("k2", "ascending")]).to_pydict()
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize("how", ["semi", "anti"])
def test_sort_merge_dtype_preservation_after_range_repartition(how, repartition_nparts, make_df, with_morsel_size):
    # Preserve dtype after repartition: ensure empty partitions do not downcast
    left = make_df(
        {"id": [1, 2, 3, None], "lv": ["a", "b", "c", "d"]}, repartition=repartition_nparts, repartition_columns=["id"]
    )
    right = make_df(
        {"id": [2, 3, 4], "rv": ["x", "y", "z"]}, repartition=repartition_nparts, repartition_columns=["id"]
    )

    out = left.join(right, left_on=["id"], right_on=["id"], how=how, strategy=JOIN_STRATEGY)
    # Select and check dtype
    df = out.select("id", "lv")
    assert df.schema()["id"].dtype == DataType.int64()
    assert df.schema()["lv"].dtype == DataType.string()
