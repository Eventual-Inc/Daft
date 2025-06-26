from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft import col
from daft.datatype import DataType
from tests.conftest import get_tests_daft_runner_name
from tests.utils import sort_arrow_table


def skip_invalid_join_strategies(join_strategy, join_type):
    if get_tests_daft_runner_name() == "native":
        if join_strategy not in [None, "hash"]:
            pytest.skip("Native executor fails for these tests")
    else:
        if (join_strategy == "sort_merge" or join_strategy == "sort_merge_aligned_boundaries") and join_type != "inner":
            pytest.skip("Sort merge currently only supports inner joins")
        elif join_strategy == "broadcast" and join_type == "outer":
            pytest.skip("Broadcast join does not support outer joins")


def test_invalid_join_strategies(make_df):
    df = make_df(
        {
            "A": [1, 2, 3],
            "B": ["a", "b", "c"],
        },
    )

    for join_type in ["left", "right", "outer"]:
        with pytest.raises(ValueError):
            df.join(df, on="A", strategy="sort_merge", how=join_type)

    with pytest.raises(ValueError):
        df.join(df, on="A", strategy="broadcast", how="outer")


def test_columns_after_join(make_df):
    df1 = make_df(
        {
            "A": [1, 2, 3],
        },
    )

    df2 = make_df({"A": [1, 2, 3], "B": [1, 2, 3]})

    joined_df1 = df1.join(df2, left_on="A", right_on="B")
    joined_df2 = df1.join(df2, left_on="A", right_on="A")

    assert set(joined_df1.schema().column_names()) == set(["A", "B", "right.A"])

    assert set(joined_df2.schema().column_names()) == set(["A", "B"])


def test_rename_join_keys_in_dataframe(make_df):
    df1 = make_df({"A": [1, 2], "B": [2, 2]})

    df2 = make_df({"A": [1, 2]})
    joined_df1 = df1.join(df2, left_on=["A", "B"], right_on=["A", "A"])
    joined_df2 = df1.join(df2, left_on=["B", "A"], right_on=["A", "A"])

    assert set(joined_df1.schema().column_names()) == set(["A", "B"])
    assert set(joined_df2.schema().column_names()) == set(["A", "B"])


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_joins(join_strategy, join_type, make_df, n_partitions: int, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    df = make_df(
        {
            "A": [1, 2, 3],
            "B": ["a", "b", "c"],
        },
        repartition=n_partitions,
        repartition_columns=["A"],
    )

    joined = df.join(df, on="A", strategy=join_strategy, how=join_type)
    # We shouldn't need to sort the joined output if using a sort-merge join.
    if join_strategy != "sort_merge":
        joined = joined.sort("A")
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1, 2, 3],
        "B": ["a", "b", "c"],
        "right.B": ["a", "b", "c"],
    }


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_multicol_joins(join_strategy, join_type, make_df, n_partitions: int, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    df = make_df(
        {
            "A": [1, 2, 3],
            "B": ["a", "b", "c"],
            "C": [True, False, True],
        },
        repartition=n_partitions,
        repartition_columns=["A", "B"],
    )

    joined = df.join(df, on=["A", "B"], strategy=join_strategy, how=join_type)
    # We shouldn't need to sort the joined output if using a sort-merge join.
    if join_strategy != "sort_merge":
        joined = joined.sort("A")
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1, 2, 3],
        "B": ["a", "b", "c"],
        "C": [True, False, True],
        "right.C": [True, False, True],
    }


@pytest.mark.parametrize("n_partitions", [1, 2, 4, 8])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_dupes_join_key(join_strategy, join_type, make_df, n_partitions: int, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    df = make_df(
        {
            "A": [1, 1, 2, 2, 3, 3],
            "B": ["a", "b", "c", "d", "e", "f"],
        },
        repartition=n_partitions,
        repartition_columns=["A"],
    )

    joined = df.join(df, on="A", strategy=join_strategy, how=join_type)
    joined = joined.sort(["A", "B", "right.B"])
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
        "B": ["a", "a", "b", "b", "c", "c", "d", "d", "e", "e", "f", "f"],
        "right.B": ["a", "b", "a", "b", "c", "d", "c", "d", "e", "f", "e", "f"],
    }


@pytest.mark.parametrize("n_partitions", [1, 2, 4, 8])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_multicol_dupes_join_key(join_strategy, join_type, make_df, n_partitions: int, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    df = make_df(
        {
            "A": [1, 1, 2, 2, 3, 3],
            "B": ["a", "a", "b", "b", "c", "d"],
            "C": [1, 0, 1, 0, 1, 0],
        },
        repartition=n_partitions,
        repartition_columns=["A", "B"],
    )

    joined = df.join(df, on=["A", "B"], strategy=join_strategy, how=join_type)
    joined = joined.sort(["A", "B", "C", "right.C"])
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3],
        "B": ["a"] * 4 + ["b"] * 4 + ["c", "d"],
        "C": [0, 0, 1, 1, 0, 0, 1, 1, 1, 0],
        "right.C": [0, 1, 0, 1, 0, 1, 0, 1, 1, 0],
    }


@pytest.mark.parametrize("n_partitions", [1, 2, 4, 6])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_joins_all_same_key(join_strategy, join_type, make_df, n_partitions: int, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    df = make_df(
        {
            "A": [1] * 4,
            "B": ["a", "b", "c", "d"],
        },
        repartition=n_partitions,
        repartition_columns=["A"],
    )

    joined = df.join(df, on="A", strategy=join_strategy, how=join_type)
    joined = joined.sort(["A", "B", "right.B"])
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1] * 16,
        "B": ["a"] * 4 + ["b"] * 4 + ["c"] * 4 + ["d"] * 4,
        "right.B": ["a", "b", "c", "d"] * 4,
    }


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,flip,expected",
    [
        ("inner", False, {"A": [], "B": [], "right.B": []}),
        ("inner", True, {"A": [], "B": [], "right.B": []}),
        (
            "left",
            False,
            {"A": [1, 2, 3], "B": ["a", "b", "c"], "right.B": [None, None, None]},
        ),
        (
            "left",
            True,
            {"A": [4, 5, 6], "B": ["d", "e", "f"], "right.B": [None, None, None]},
        ),
        (
            "right",
            False,
            {"A": [4, 5, 6], "B": [None, None, None], "right.B": ["d", "e", "f"]},
        ),
        (
            "right",
            True,
            {"A": [1, 2, 3], "B": [None, None, None], "right.B": ["a", "b", "c"]},
        ),
        (
            "outer",
            False,
            {
                "A": [1, 2, 3, 4, 5, 6],
                "B": ["a", "b", "c", None, None, None],
                "right.B": [None, None, None, "d", "e", "f"],
            },
        ),
        (
            "outer",
            True,
            {
                "A": [1, 2, 3, 4, 5, 6],
                "B": [None, None, None, "d", "e", "f"],
                "right.B": ["a", "b", "c", None, None, None],
            },
        ),
    ],
)
def test_joins_no_overlap_disjoint(
    join_strategy, join_type, flip, expected, make_df, n_partitions: int, with_morsel_size
):
    skip_invalid_join_strategies(join_strategy, join_type)

    df1 = make_df(
        {
            "A": [1, 2, 3],
            "B": ["a", "b", "c"],
        },
        repartition=n_partitions,
        repartition_columns=["A"],
    )
    df2 = make_df(
        {
            "A": [4, 5, 6],
            "B": ["d", "e", "f"],
        },
        repartition=n_partitions,
        repartition_columns=["A"],
    )

    if flip:
        joined = df2.join(df1, on="A", strategy=join_strategy, how=join_type)
    else:
        joined = df1.join(df2, on="A", strategy=join_strategy, how=join_type)
    joined = joined.sort("A")
    joined_data = joined.to_pydict()

    assert joined_data == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,flip,expected",
    [
        ("inner", False, {"A": [], "B": [], "right.B": []}),
        ("inner", True, {"A": [], "B": [], "right.B": []}),
        (
            "left",
            False,
            {"A": [1, 3, 5], "B": ["a", "b", "c"], "right.B": [None, None, None]},
        ),
        (
            "left",
            True,
            {"A": [2, 4, 6], "B": ["d", "e", "f"], "right.B": [None, None, None]},
        ),
        (
            "right",
            False,
            {"A": [2, 4, 6], "B": [None, None, None], "right.B": ["d", "e", "f"]},
        ),
        (
            "right",
            True,
            {"A": [1, 3, 5], "B": [None, None, None], "right.B": ["a", "b", "c"]},
        ),
        (
            "outer",
            False,
            {
                "A": [1, 2, 3, 4, 5, 6],
                "B": ["a", None, "b", None, "c", None],
                "right.B": [None, "d", None, "e", None, "f"],
            },
        ),
        (
            "outer",
            True,
            {
                "A": [1, 2, 3, 4, 5, 6],
                "B": [None, "d", None, "e", None, "f"],
                "right.B": ["a", None, "b", None, "c", None],
            },
        ),
    ],
)
def test_joins_no_overlap_interleaved(
    join_strategy, join_type, flip, expected, make_df, n_partitions: int, with_morsel_size
):
    skip_invalid_join_strategies(join_strategy, join_type)

    df1 = make_df(
        {
            "A": [1, 3, 5],
            "B": ["a", "b", "c"],
        },
        repartition=n_partitions,
        repartition_columns=["A"],
    )
    df2 = make_df(
        {
            "A": [2, 4, 6],
            "B": ["d", "e", "f"],
        },
        repartition=n_partitions,
        repartition_columns=["A"],
    )

    if flip:
        joined = df2.join(df1, on="A", strategy=join_strategy, how=join_type)
    else:
        joined = df1.join(df2, on="A", strategy=join_strategy, how=join_type)
    # We shouldn't need to sort the joined output if using a sort-merge join.
    if join_strategy != "sort_merge":
        joined = joined.sort("A")
    joined_data = joined.to_pydict()

    assert joined_data == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_limit_after_join(join_strategy, join_type, make_df, n_partitions: int, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    data = {
        "A": [1, 2, 3],
    }
    df1 = make_df(
        data,
        repartition=n_partitions,
        repartition_columns=["A"],
    )
    df2 = make_df(
        data,
        repartition=n_partitions,
        repartition_columns=["A"],
    )

    joined = df1.join(df2, on="A", strategy=join_strategy, how=join_type).limit(1)
    joined_data = joined.to_pydict()
    assert "A" in joined_data
    assert len(joined_data["A"]) == 1


###
# Tests for nulls
###


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        (
            "inner",
            {"id": [1, 3], "values_left": ["a1", "c1"], "values_right": ["a2", "c2"]},
        ),
        (
            "left",
            {
                "id": [1, 3, None],
                "values_left": ["a1", "c1", "b1"],
                "values_right": ["a2", "c2", None],
            },
        ),
        (
            "right",
            {
                "id": [1, 2, 3],
                "values_left": ["a1", None, "c1"],
                "values_right": ["a2", "b2", "c2"],
            },
        ),
        (
            "outer",
            {
                "id": [1, 2, 3, None],
                "values_left": ["a1", None, "c1", "b1"],
                "values_right": ["a2", "b2", "c2", None],
            },
        ),
    ],
)
def test_join_with_null(join_strategy, join_type, expected, make_df, repartition_nparts, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df = make_df(
        {
            "id": [1, None, 3],
            "values_left": ["a1", "b1", "c1"],
        },
        repartition=repartition_nparts,
    )
    daft_df2 = make_df(
        {
            "id": [1, 2, 3],
            "values_right": ["a2", "b2", "c2"],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.join(daft_df2, on="id", strategy=join_strategy, how=join_type)

    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "id") == sort_arrow_table(
        pa.Table.from_pydict(expected), "id"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        (
            "inner",
            {"id": [1], "id2": ["foo1"], "values_left": ["a1"], "values_right": ["c2"]},
        ),
        (
            "left",
            {
                "id": [1, None, None],
                "id2": ["foo1", "foo2", None],
                "values_left": ["a1", "b1", "c1"],
                "values_right": ["c2", None, None],
            },
        ),
        (
            "right",
            {
                "id": [1, None, None],
                "id2": ["foo1", "foo2", None],
                "values_left": ["a1", None, None],
                "values_right": ["c2", "a2", "b2"],
            },
        ),
        (
            "outer",
            {
                "id": [1, None, None, None, None],
                "id2": ["foo1", "foo2", "foo2", None, None],
                "values_left": ["a1", "b1", None, "c1", None],
                "values_right": ["c2", None, "a2", None, "b2"],
            },
        ),
    ],
)
def test_join_with_null_multikey(join_strategy, join_type, expected, make_df, repartition_nparts, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df = make_df(
        {
            "id": [1, None, None],
            "id2": ["foo1", "foo2", None],
            "values_left": ["a1", "b1", "c1"],
        },
        repartition=repartition_nparts,
    )
    daft_df2 = make_df(
        {
            "id": [None, None, 1],
            "id2": ["foo2", None, "foo1"],
            "values_right": ["a2", "b2", "c2"],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.join(daft_df2, on=["id", "id2"], strategy=join_strategy, how=join_type).sort(
        ["id", "id2", "values_left", "values_right"]
    )

    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "id") == sort_arrow_table(
        pa.Table.from_pydict(expected), "id"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        (
            "inner",
            {
                "left_id": [1],
                "left_id2": ["foo1"],
                "values_left": ["a1"],
                "right_id": [1],
                "right_id2": ["foo1"],
                "values_right": ["c2"],
            },
        ),
        (
            "left",
            {
                "left_id": [1, None, None],
                "left_id2": ["foo1", "foo2", None],
                "values_left": ["a1", "b1", "c1"],
                "right_id": [1, None, None],
                "right_id2": ["foo1", None, None],
                "values_right": ["c2", None, None],
            },
        ),
        (
            "right",
            {
                "left_id": [1, None, None],
                "left_id2": ["foo1", None, None],
                "values_left": ["a1", None, None],
                "right_id": [1, None, None],
                "right_id2": ["foo1", "foo2", None],
                "values_right": ["c2", "a2", "b2"],
            },
        ),
        (
            "outer",
            {
                "left_id": [1, None, None, None, None],
                "left_id2": ["foo1", "foo2", None, None, None],
                "values_left": ["a1", "b1", None, "c1", None],
                "right_id": [1, None, None, None, None],
                "right_id2": ["foo1", None, "foo2", None, None],
                "values_right": ["c2", None, "a2", None, "b2"],
            },
        ),
    ],
)
def test_join_with_null_asymmetric_multikey(
    join_strategy, join_type, expected, make_df, repartition_nparts, with_morsel_size
):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df = make_df(
        {
            "left_id": [1, None, None],
            "left_id2": ["foo1", "foo2", None],
            "values_left": ["a1", "b1", "c1"],
        },
        repartition=repartition_nparts,
    )
    daft_df2 = make_df(
        {
            "right_id": [None, None, 1],
            "right_id2": ["foo2", None, "foo1"],
            "values_right": ["a2", "b2", "c2"],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.join(
        daft_df2,
        left_on=["left_id", "left_id2"],
        right_on=["right_id", "right_id2"],
        how=join_type,
        strategy=join_strategy,
    ).sort(["left_id", "left_id2", "right_id", "right_id2", "values_left", "values_right"])

    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "left_id") == sort_arrow_table(
        pa.Table.from_pydict(expected), "left_id"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        (
            "inner",
            {
                "id": [],
                "values_left": [],
                "values_right": [],
            },
        ),
        (
            "left",
            {
                "id": [None, None, None],
                "values_left": ["a1", "b1", "c1"],
                "values_right": [None, None, None],
            },
        ),
        (
            "right",
            {
                "id": [1, 2, 3],
                "values_left": [None, None, None],
                "values_right": ["a2", "b2", "c2"],
            },
        ),
        (
            "outer",
            {
                "id": [1, 2, 3, None, None, None],
                "values_left": [None, None, None, "a1", "b1", "c1"],
                "values_right": ["a2", "b2", "c2", None, None, None],
            },
        ),
    ],
)
def test_join_all_null(join_strategy, join_type, expected, make_df, repartition_nparts, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df = make_df(
        {
            "id": [None, None, None],
            "values_left": ["a1", "b1", "c1"],
        },
        repartition=repartition_nparts,
    )
    daft_df2 = make_df(
        {
            "id": [1, 2, 3],
            "values_right": ["a2", "b2", "c2"],
        },
        repartition=repartition_nparts,
    )
    daft_df = (
        daft_df.with_column("id", daft_df["id"].cast(DataType.int64()))
        .join(daft_df2, on="id", how=join_type, strategy=join_strategy)
        .sort(["id", "values_left", "values_right"])
    )

    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "id") == sort_arrow_table(
        pa.Table.from_pydict(expected), "id"
    )


@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        ("inner", {"id": [], "values_left": [], "values_right": []}),
        ("left", {"id": [None, None, None], "values_left": ["a1", "b1", "c1"], "values_right": [None, None, None]}),
        ("right", {"id": [None, None, None], "values_left": [None, None, None], "values_right": ["a2", "b2", "c2"]}),
        (
            "outer",
            {
                "id": [None, None, None, None, None, None],
                "values_left": ["a1", "b1", "c1", None, None, None],
                "values_right": [None, None, None, "a2", "b2", "c2"],
            },
        ),
    ],
)
def test_join_null_type_column(join_strategy, join_type, expected, make_df, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df = make_df(
        {
            "id": [None, None, None],
            "values_left": ["a1", "b1", "c1"],
        }
    )
    daft_df2 = make_df(
        {
            "id": [None, None, None],
            "values_right": ["a2", "b2", "c2"],
        }
    )

    daft_df = daft_df.join(daft_df2, on="id", how=join_type, strategy=join_strategy).sort(
        ["values_left", "values_right"]
    )
    assert pa.Table.from_pydict(daft_df.to_pydict()) == pa.Table.from_pydict(expected)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        (
            "semi",
            {
                "id": [2, 3],
                "values_left": ["b1", "c1"],
            },
        ),
        (
            "anti",
            {
                "id": [1, None],
                "values_left": ["a1", "d1"],
            },
        ),
    ],
)
def test_join_semi_anti(join_strategy, join_type, expected, make_df, repartition_nparts, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df1 = make_df(
        {
            "id": [1, 2, 3, None],
            "values_left": ["a1", "b1", "c1", "d1"],
        },
        repartition=repartition_nparts,
    )
    daft_df2 = make_df(
        {
            "id": [2, 2, 3, 4],
            "values_right": ["a2", "b2", "c2", "d2"],
        },
        repartition=repartition_nparts,
    )
    daft_df = (
        daft_df1.with_column("id", daft_df1["id"].cast(DataType.int64()))
        .join(daft_df2, on="id", how=join_type, strategy=join_strategy)
        .sort(["id", "values_left"])
    ).select("id", "values_left")

    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "id") == sort_arrow_table(
        pa.Table.from_pydict(expected), "id"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        (
            "semi",
            {
                "id_left": [2, 3],
                "values_left": ["b1", "c1"],
            },
        ),
        (
            "anti",
            {
                "id_left": [1, None],
                "values_left": ["a1", "d1"],
            },
        ),
    ],
)
def test_join_semi_anti_different_names(
    join_strategy, join_type, expected, make_df, repartition_nparts, with_morsel_size
):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df1 = make_df(
        {
            "id_left": [1, 2, 3, None],
            "values_left": ["a1", "b1", "c1", "d1"],
        },
        repartition=repartition_nparts,
    )
    daft_df2 = make_df(
        {
            "id_right": [2, 2, 3, 4],
            "values_right": ["a2", "b2", "c2", "d2"],
        },
        repartition=repartition_nparts,
    )
    daft_df = (
        daft_df1.with_column("id_left", daft_df1["id_left"].cast(DataType.int64()))
        .join(
            daft_df2,
            left_on="id_left",
            right_on="id_right",
            how=join_type,
            strategy=join_strategy,
        )
        .sort(["id_left", "values_left"])
    ).select("id_left", "values_left")

    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "id_left") == sort_arrow_table(
        pa.Table.from_pydict(expected), "id_left"
    )


@pytest.mark.parametrize(
    "join_type,expected_dtypes",
    [
        (
            "inner",
            {
                "id": DataType.int64(),
                "values": DataType.string(),
                "right.values": DataType.string(),
            },
        ),
        (
            "left",
            {
                "id": DataType.int64(),
                "values": DataType.string(),
                "right.values": DataType.string(),
            },
        ),
        (
            "right",
            {
                "id": DataType.float64(),
                "values": DataType.string(),
                "right.values": DataType.string(),
            },
        ),
        (
            "outer",
            {
                "id": DataType.float64(),
                "values": DataType.string(),
                "right.values": DataType.string(),
            },
        ),
    ],
)
def test_join_true_join_keys(join_type, expected_dtypes, make_df, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 2, 3],
            "values": ["a", "b", "c"],
        }
    )
    daft_df2 = make_df(
        {
            "id": [2.0, 2.5, 3.0, 4.0],
            "values": ["a2", "b2", "c2", "d2"],
        }
    )

    result = daft_df.join(daft_df2, left_on=["id", "values"], right_on=["id", col("values").str.left(1)], how=join_type)

    assert result.schema().column_names() == ["id", "values", "right.values"]
    assert result.schema()["id"].dtype == expected_dtypes["id"]
    assert result.schema()["values"].dtype == expected_dtypes["values"]
    assert result.schema()["right.values"].dtype == expected_dtypes["right.values"]


@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        (
            "inner",
            {
                "a": [2, 3],
                "b": [2, 3],
            },
        ),
        (
            "left",
            {
                "a": [1, 2, 3],
                "b": [None, 2, 3],
            },
        ),
        (
            "right",
            {
                "a": [2, 3, None],
                "b": [2, 3, 4],
            },
        ),
        (
            "outer",
            {
                "a": [1, 2, 3, None],
                "b": [None, 2, 3, 4],
            },
        ),
        (
            "semi",
            {
                "a": [2, 3],
            },
        ),
        (
            "anti",
            {
                "a": [1],
            },
        ),
    ],
)
def test_join_with_alias_in_key(join_strategy, join_type, expected, make_df, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df1 = make_df(
        {
            "a": [1, 2, 3],
        }
    )
    daft_df2 = make_df(
        {
            "b": [2, 3, 4],
        }
    )

    daft_df = daft_df1.join(daft_df2, left_on=col("a").alias("x"), right_on="b", how=join_type, strategy=join_strategy)

    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "a") == sort_arrow_table(
        pa.Table.from_pydict(expected), "a"
    )


@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        (
            "inner",
            {
                "a": [2, 3],
                "right.a": [2, 3],
            },
        ),
        (
            "left",
            {
                "a": [1, 2, 3],
                "right.a": [None, 2, 3],
            },
        ),
        (
            "right",
            {
                "a": [2, 3, None],
                "right.a": [2, 3, 4],
            },
        ),
        (
            "outer",
            {
                "a": [1, 2, 3, None],
                "right.a": [None, 2, 3, 4],
            },
        ),
        (
            "semi",
            {
                "a": [2, 3],
            },
        ),
        (
            "anti",
            {
                "a": [1],
            },
        ),
    ],
)
def test_join_same_name_alias(join_strategy, join_type, expected, make_df, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df1 = make_df(
        {
            "a": [1, 2, 3],
        }
    )
    daft_df2 = make_df(
        {
            "a": [2, 3, 4],
        }
    )

    daft_df = daft_df1.join(daft_df2, left_on="a", right_on=col("a").alias("b"), how=join_type, strategy=join_strategy)

    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "a") == sort_arrow_table(
        pa.Table.from_pydict(expected), "a"
    )


@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize(
    "join_type,expected",
    [
        (
            "inner",
            {
                "a": [0.2, 0.3],
                "right.a": [20, 30],
            },
        ),
        (
            "left",
            {
                "a": [0.1, 0.2, 0.3],
                "right.a": [None, 20, 30],
            },
        ),
        (
            "right",
            {
                "a": [0.2, 0.3, None],
                "right.a": [20, 30, 40],
            },
        ),
        (
            "outer",
            {
                "a": [0.1, 0.2, 0.3, None],
                "right.a": [None, 20, 30, 40],
            },
        ),
        (
            "semi",
            {
                "a": [0.2, 0.3],
            },
        ),
        (
            "anti",
            {
                "a": [0.1],
            },
        ),
    ],
)
def test_join_same_name_alias_with_compute(join_strategy, join_type, expected, make_df, with_morsel_size):
    skip_invalid_join_strategies(join_strategy, join_type)

    daft_df1 = make_df(
        {
            "a": [0.1, 0.2, 0.3],
        }
    )
    daft_df2 = make_df(
        {
            "a": [20, 30, 40],
        }
    )

    daft_df = daft_df1.join(
        daft_df2, left_on=col("a") * 10, right_on=(col("a") / 10).alias("b"), how=join_type, strategy=join_strategy
    )

    assert sort_arrow_table(pa.Table.from_pydict(daft_df.to_pydict()), "a") == sort_arrow_table(
        pa.Table.from_pydict(expected), "a"
    )


@pytest.mark.parametrize(
    "suffix,prefix,expected",
    [
        (None, None, "right.score"),
        ("_right", None, "score_right"),
        (None, "left_", "left_score"),
        ("_right", "prefix.", "prefix.score_right"),
    ],
)
def test_join_suffix_and_prefix(suffix, prefix, expected, make_df, with_morsel_size):
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [3], "score": [0.1]})
    df3 = daft.from_pydict({"idx": [1], "score": [0.1]})

    df = df1.join(df2, on="idx").join(df3, on="idx", suffix=suffix, prefix=prefix)
    assert df.column_names == ["idx", "val", "score", expected]


@pytest.mark.parametrize("left_partitions", [1, 2, 4])
@pytest.mark.parametrize("right_partitions", [1, 2, 4])
def test_cross_join(left_partitions, right_partitions, make_df, with_morsel_size):
    df1 = make_df(
        {
            "A": [1, 3, 5],
            "B": ["a", "b", "c"],
        },
        repartition=left_partitions,
        repartition_columns=["A"],
    )

    df2 = make_df(
        {
            "C": [2, 4, 6, 8],
            "D": ["d", "e", "f", "g"],
        },
        repartition=right_partitions,
        repartition_columns=["C"],
    )

    df = df1.join(df2, how="cross")
    df = df.sort(["A", "C"])

    assert df.to_pydict() == {
        "A": [1, 1, 1, 1, 3, 3, 3, 3, 5, 5, 5, 5],
        "B": ["a", "a", "a", "a", "b", "b", "b", "b", "c", "c", "c", "c"],
        "C": [2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8],
        "D": ["d", "e", "f", "g", "d", "e", "f", "g", "d", "e", "f", "g"],
    }


@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer", "anti", "semi", "cross"])
@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize(
    "left,right,expected",
    [
        # Case 1: Left has data, right is empty
        (
            {"a": [1, 2, 3, 4], "b": ["a", "b", "c", "d"]},
            {"c": [], "d": []},
            {
                "inner": {"a": [], "b": [], "c": [], "d": []},
                "left": {
                    "a": [1, 2, 3, 4],
                    "b": ["a", "b", "c", "d"],
                    "c": [None, None, None, None],
                    "d": [None, None, None, None],
                },
                "right": {"a": [], "b": [], "c": [], "d": []},
                "outer": {
                    "a": [1, 2, 3, 4],
                    "b": ["a", "b", "c", "d"],
                    "c": [None, None, None, None],
                    "d": [None, None, None, None],
                },
                "anti": {"a": [1, 2, 3, 4], "b": ["a", "b", "c", "d"]},
                "semi": {"a": [], "b": []},
                "cross": {"a": [], "b": [], "c": [], "d": []},
            },
        ),
        # Case 2: Left is empty, right has data
        (
            {"a": [], "b": []},
            {"c": [5, 6, 7], "d": ["e", "f", "g"]},
            {
                "inner": {"a": [], "b": [], "c": [], "d": []},
                "left": {"a": [], "b": [], "c": [], "d": []},
                "right": {"a": [None, None, None], "b": [None, None, None], "c": [5, 6, 7], "d": ["e", "f", "g"]},
                "outer": {"a": [None, None, None], "b": [None, None, None], "c": [5, 6, 7], "d": ["e", "f", "g"]},
                "anti": {"a": [], "b": []},
                "semi": {"a": [], "b": []},
                "cross": {"a": [], "b": [], "c": [], "d": []},
            },
        ),
        # Case 3: Both empty
        (
            {"a": [], "b": []},
            {"c": [], "d": []},
            {
                "inner": {"a": [], "b": [], "c": [], "d": []},
                "left": {"a": [], "b": [], "c": [], "d": []},
                "right": {"a": [], "b": [], "c": [], "d": []},
                "outer": {"a": [], "b": [], "c": [], "d": []},
                "anti": {"a": [], "b": []},
                "semi": {"a": [], "b": []},
                "cross": {"a": [], "b": [], "c": [], "d": []},
            },
        ),
    ],
)
def test_join_empty(join_type, repartition_nparts, left, right, expected, make_df, with_morsel_size):
    left_df = make_df(
        left,
        repartition=repartition_nparts,
        repartition_columns=["a"],
    )

    right_df = make_df(
        right,
        repartition=repartition_nparts,
        repartition_columns=["c"],
    )

    if join_type == "cross":
        left_on = None
        right_on = None
    else:
        left_on = ["a"]
        right_on = ["c"]

    result = left_df.join(right_df, left_on=left_on, right_on=right_on, how=join_type)

    sort_by = ["a", "b", "c", "d"] if join_type in ["inner", "left", "right", "outer", "cross"] else ["a", "b"]

    assert sort_arrow_table(pa.Table.from_pydict(result.to_pydict()), *sort_by) == sort_arrow_table(
        pa.Table.from_pydict(expected[join_type]), *sort_by
    )


@pytest.mark.parametrize(
    "join_type,expected",
    [
        ("inner", {"a": [1, 1, 2], "b": ["a", "a", "b"], "c": [1.0, 1.0, 2.0], "d": ["g", "h", "j"]}),
        (
            "left",
            {
                "a": [1, 1, 2, 3, 4, 5, 6],
                "b": ["a", "a", "b", "c", "d", "e", "f"],
                "c": [1.0, 1.0, 2.0, None, None, None, None],
                "d": ["g", "h", "j", None, None, None, None],
            },
        ),
        (
            "right",
            {
                "a": [1, 1, 2, None, None, None],
                "b": ["a", "a", "b", None, None, None],
                "c": [1.0, 1.0, 2.0, 1.5, 2.1, 2.5],
                "d": ["g", "h", "j", "i", "k", "l"],
            },
        ),
        (
            "outer",
            {
                "a": [1, 1, 2, 3, 4, 5, 6, None, None, None],
                "b": ["a", "a", "b", "c", "d", "e", "f", None, None, None],
                "c": [1.0, 1.0, 2.0, None, None, None, None, 1.5, 2.1, 2.5],
                "d": ["g", "h", "j", None, None, None, None, "i", "k", "l"],
            },
        ),
        (
            "anti",
            {
                "a": [3, 4, 5, 6],
                "b": ["c", "d", "e", "f"],
            },
        ),
        (
            "semi",
            {
                "a": [1, 2],
                "b": ["a", "b"],
            },
        ),
    ],
)
@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_join_different_join_key_types(join_type, expected, repartition_nparts, make_df, with_morsel_size):
    left_df = make_df(
        {"a": [1, 2, 3, 4, 5, 6], "b": ["a", "b", "c", "d", "e", "f"]},
        repartition=repartition_nparts,
        repartition_columns=["a"],
    )

    right_df = make_df(
        {"c": [1.0, 1.0, 1.5, 2.0, 2.1, 2.5], "d": ["g", "h", "i", "j", "k", "l"]},
        repartition=repartition_nparts,
        repartition_columns=["c"],
    )

    result = left_df.join(right_df, left_on="a", right_on="c", how=join_type)

    sort_by = ["a", "b", "c", "d"] if join_type in ["inner", "left", "right", "outer"] else ["a", "b"]

    assert sort_arrow_table(pa.Table.from_pydict(result.to_pydict()), *sort_by) == sort_arrow_table(
        pa.Table.from_pydict(expected), *sort_by
    )


# Tests issue 4113.
def test_self_join_with_projection():
    df1 = daft.from_pydict(
        {
            "pattern_id_l": ["a"],
            "pattern_id_r": ["b"],
        }
    )

    df2 = daft.from_pydict(
        {
            "id": ["1", "2", "3"],
            "pattern_id": ["a", "b", "c"],
        }
    )

    result_df = (
        df1.join(
            df2,
            left_on="pattern_id_l",
            right_on="pattern_id",
        )
        .select(daft.col("id").alias("id_l"), daft.col("pattern_id_r"))
        .join(
            df2,
            left_on="pattern_id_r",
            right_on="pattern_id",
        )
        .select(
            daft.col("id_l"),
            daft.col("id").alias("id_r"),
        )
    )

    expected = {
        "id_l": ["1"],
        "id_r": ["2"],
    }

    assert pa.Table.from_pydict(result_df.to_pydict()) == pa.Table.from_pydict(expected)
