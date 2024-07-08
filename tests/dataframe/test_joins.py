from __future__ import annotations

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.errors import ExpressionTypeError
from tests.utils import sort_arrow_table


def skip_invalid_join_strategies(join_strategy, join_type):
    if (join_strategy == "sort_merge" or join_strategy == "sort_merge_aligned_boundaries") and join_type != "inner":
        pytest.skip("Sort merge currently only supports inner joins")
    elif join_strategy == "broadcast" and join_type == "outer":
        pytest.skip("Broadcast join does not support outer joins")
    elif join_strategy == "broadcast" and join_type == "anti":
        pytest.skip("Broadcast join does not support anti joins")
    elif join_strategy == "broadcast" and join_type == "semi":
        pytest.skip("Broadcast join does not support semi joins")


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


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_joins(join_strategy, join_type, make_df, n_partitions: int):
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
def test_multicol_joins(join_strategy, join_type, make_df, n_partitions: int):
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
def test_dupes_join_key(join_strategy, join_type, make_df, n_partitions: int):
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
    joined = joined.sort(["A", "B"])
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
def test_multicol_dupes_join_key(join_strategy, join_type, make_df, n_partitions: int):
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
    joined = joined.sort(["A", "B", "C"])
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3],
        "B": ["a"] * 4 + ["b"] * 4 + ["c", "d"],
        "C": [0, 0, 1, 1, 0, 0, 1, 1, 1, 0],
        "right.C": [1, 0, 1, 0, 1, 0, 1, 0, 1, 0],
    }


@pytest.mark.parametrize("n_partitions", [1, 2, 4, 6])
@pytest.mark.parametrize(
    "join_strategy",
    [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"],
    indirect=True,
)
@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_joins_all_same_key(join_strategy, join_type, make_df, n_partitions: int):
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
    joined = joined.sort(["A", "B"])
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
def test_joins_no_overlap_disjoint(join_strategy, join_type, flip, expected, make_df, n_partitions: int):
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
def test_joins_no_overlap_interleaved(join_strategy, join_type, flip, expected, make_df, n_partitions: int):
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
def test_limit_after_join(join_strategy, join_type, make_df, n_partitions: int):
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
def test_join_with_null(join_strategy, join_type, expected, make_df, repartition_nparts):
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
def test_join_with_null_multikey(join_strategy, join_type, expected, make_df, repartition_nparts):
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
def test_join_with_null_asymmetric_multikey(join_strategy, join_type, expected, make_df, repartition_nparts):
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
def test_join_all_null(join_strategy, join_type, expected, make_df, repartition_nparts):
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
@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_join_null_type_column(join_strategy, join_type, make_df):
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

    with pytest.raises((ExpressionTypeError, ValueError)):
        daft_df.join(daft_df2, on="id", how=join_type, strategy=join_strategy)


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
def test_join_semi_anti(join_strategy, join_type, expected, make_df, repartition_nparts):
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
def test_join_semi_anti_different_names(join_strategy, join_type, expected, make_df, repartition_nparts):
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
