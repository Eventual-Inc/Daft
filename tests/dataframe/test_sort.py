from __future__ import annotations

import math

import pyarrow as pa
import pytest

import daft
from daft.datatype import DataType
from daft.errors import ExpressionTypeError

###
# Validation tests
###


def test_disallowed_sort_null(make_df):
    df = make_df({"A": [None, None]})

    with pytest.raises((ExpressionTypeError, ValueError)):
        df.sort("A")


def test_disallowed_sort_bytes(make_df):
    df = make_df({"A": [b"a", b"b"]})

    with pytest.raises((ExpressionTypeError, ValueError)):
        df.sort("A")


###
# Functional tests
###


@pytest.mark.parametrize("desc", [True, False])
@pytest.mark.parametrize("n_partitions", [1, 3])
def test_single_float_col_sort(make_df, desc: bool, n_partitions: int, with_morsel_size):
    df = make_df({"A": [1.0, None, 3.0, float("nan"), 2.0]}, repartition=n_partitions)
    df = df.sort("A", desc=desc)
    sorted_data = df.to_pydict()

    def _replace_nan_with_string(items):
        return ["nan" if isinstance(item, float) and math.isnan(item) else item for item in items]

    expected = [1.0, 2.0, 3.0, float("nan"), None]
    if desc:
        expected = list(reversed(expected))

    assert _replace_nan_with_string(sorted_data["A"]) == _replace_nan_with_string(expected)


@pytest.mark.skip(reason="Issue: https://github.com/Eventual-Inc/Daft/issues/546")
@pytest.mark.parametrize("n_partitions", [1, 3])
def test_multi_float_col_sort(make_df, n_partitions: int, with_morsel_size):
    df = make_df(
        {
            "A": [1.0, 1.0, None, None, float("nan"), float("nan"), float("nan")],
            "B": [1.0, 2.0, float("nan"), None, None, float("nan"), 1.0],
        },
        repartition=n_partitions,
    )
    df = df.sort(["A", "B"], desc=[True, False])
    sorted_data = df.to_pydict()

    def _replace_nan_with_string(items):
        return ["nan" if isinstance(item, float) and math.isnan(item) else item for item in items]

    expected = {
        "A": [
            # Descending: None is largest value
            None,
            None,
            # Descending: nan is smaller than None
            float("nan"),
            float("nan"),
            float("nan"),
            # Descending: values follow
            1.0,
            1.0,
        ],
        "B": [
            # Ascending: nan smaller than None
            float("nan"),
            None,
            # Ascending: values followed by nan then None
            1.0,
            float("nan"),
            None,
            # Ascending: sorted order for values
            1.0,
            2.0,
        ],
    }

    assert _replace_nan_with_string(sorted_data["A"]) == _replace_nan_with_string(expected["A"])
    assert _replace_nan_with_string(sorted_data["B"]) == _replace_nan_with_string(expected["B"])


@pytest.mark.parametrize("desc", [True, False])
@pytest.mark.parametrize("n_partitions", [1, 3])
def test_single_string_col_sort(make_df, desc: bool, n_partitions: int, with_morsel_size):
    df = make_df({"A": ["0", None, "1", "", "01"]}, repartition=n_partitions)
    df = df.sort("A", desc=desc)
    sorted_data = df.to_pydict()

    expected = ["", "0", "01", "1", None]
    if desc:
        expected = list(reversed(expected))

    assert sorted_data["A"] == expected


@pytest.mark.parametrize("desc", [True, False])
@pytest.mark.parametrize("n_partitions", [1, 3, 4])
def test_single_bool_col_sort(make_df, desc: bool, n_partitions: int, with_morsel_size):
    df = make_df({"A": [True, None, False, True, False]}, repartition=n_partitions)
    df = df.sort("A", desc=desc)
    sorted_data = df.to_pydict()

    expected = [False, False, True, True, None]
    if desc:
        expected = list(reversed(expected))

    assert sorted_data["A"] == expected


@pytest.mark.parametrize("n_partitions", [1, 3, 4])
def test_multi_bool_col_sort(make_df, n_partitions: int, with_morsel_size):
    df = make_df(
        {
            "A": [True, False, None, False, True],
            "B": [None, True, False, True, None],
        },
        repartition=n_partitions,
    )
    df = df.sort(["A", "B"], desc=[True, False])
    sorted_data = df.to_pydict()

    expected = {
        "A": [None, True, True, False, False],
        "B": [False, None, None, True, True],
    }

    assert sorted_data["A"] == expected["A"]
    assert sorted_data["B"] == expected["B"]


###
# Null tests
###


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_int_sort_with_nulls(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [2, None, 1],
            "values": ["a1", "b1", "c1"],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.sort(daft_df["id"])

    expected = pa.Table.from_pydict(
        {
            "id": [1, 2, None],
            "values": ["c1", "a1", "b1"],
        }
    )
    daft_df.collect()

    assert pa.Table.from_pydict(daft_df.to_pydict()) == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_str_sort_with_nulls(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, None, 2],
            "values": ["c1", None, "a1"],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.sort(daft_df["values"])

    expected = pa.Table.from_pydict(
        {
            "id": [2, 1, None],
            "values": ["a1", "c1", None],
        }
    )
    daft_df.collect()
    assert pa.Table.from_pydict(daft_df.to_pydict()) == expected


@pytest.mark.parametrize("repartition_nparts", [1, 4, 6])
def test_sort_with_nulls_multikey(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id1": [2, None, 2, None, 1],
            "id2": [2, None, 1, 1, None],
            "values": ["a1", "b1", "c1", "d1", "e1"],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.sort([daft_df["id1"], daft_df["id2"]])

    expected = pa.Table.from_pydict(
        {
            "id1": [1, 2, 2, None, None],
            "id2": [None, 1, 2, 1, None],
            "values": ["e1", "c1", "a1", "d1", "b1"],
        }
    )
    daft_df.collect()
    assert pa.Table.from_pydict(daft_df.to_pydict()) == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_sort_with_all_nulls(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [None, None, None],
            "values": ["c1", None, "a1"],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.with_column("id", daft_df["id"].cast(DataType.int64())).sort(daft_df["id"])
    daft_df.collect()

    resultset = daft_df.to_pydict()
    assert len(resultset["id"]) == 3
    assert len(resultset["values"]) == 3


@pytest.mark.parametrize("repartition_nparts", [1, 2])
def test_sort_with_empty(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1],
            "values": ["a1"],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.where(daft_df["id"] != 1).sort(daft_df["id"])
    daft_df.collect()

    resultset = daft_df.to_pydict()
    assert len(resultset["id"]) == 0
    assert len(resultset["values"]) == 0


def test_sort_with_all_null_type_column(make_df):
    daft_df = make_df(
        {
            "id": [None, None, None],
            "values": ["a1", "b1", "c1"],
        }
    )

    with pytest.raises((ExpressionTypeError, ValueError)):
        daft_df = daft_df.sort(daft_df["id"])


def test_sort_nulls_first(make_df):
    df = make_df({"A": [1, None, 3, None, 2]})

    result_nulls_first = df.sort("A", nulls_first=True).to_pydict()
    assert result_nulls_first["A"] == [None, None, 1, 2, 3]

    result_nulls_last = df.sort("A", nulls_first=False).to_pydict()
    assert result_nulls_last["A"] == [1, 2, 3, None, None]


def test_sort_desc_nulls_first(make_df):
    df = make_df({"A": [1, None, 3, None, 2]})

    result = df.sort("A", desc=True, nulls_first=True).to_pydict()
    assert result["A"] == [None, None, 3, 2, 1]


@pytest.mark.parametrize(
    "desc,nulls_first,expected_data",
    [
        (True, True, {"b": [None, None, 10, 5, 1]}),
        (True, False, {"b": [10, 5, 1, None, None]}),
        (False, True, {"b": [None, None, 1, 5, 10]}),
        (False, False, {"b": [1, 5, 10, None, None]}),
    ],
)
def test_sort_combinations(desc, nulls_first, expected_data):
    data = {"b": [1, 10, 5, None, None]}
    df = daft.from_pydict(data)
    actual = df.sort(by="b", desc=desc, nulls_first=nulls_first)
    expected = daft.from_pydict(expected_data)
    assert actual.to_pydict() == expected.to_pydict()


@pytest.mark.parametrize(
    "desc,nulls_first,expected",
    [
        pytest.param(
            [False, False],
            [False, False],
            {"a": [0, 1, -1, 10, None, None, 999], "id": [1, 2, 3, 3, 3, 4, None]},
            id="asc asc, last last",
        ),
        pytest.param(
            [False, False],
            [False, True],
            {"a": [0, 1, None, -1, 10, None, 999], "id": [1, 2, 3, 3, 3, 4, None]},
            id="asc asc, last first",
        ),
        pytest.param(
            [False, False],
            [True, False],
            {"a": [999, 0, 1, -1, 10, None, None], "id": [None, 1, 2, 3, 3, 3, 4]},
            id="asc asc, first last",
        ),
        pytest.param(
            [False, False],
            [True, True],
            {"a": [999, 0, 1, None, -1, 10, None], "id": [None, 1, 2, 3, 3, 3, 4]},
            id="asc asc, first first",
        ),
        pytest.param(
            [True, False],
            [False, False],
            {"a": [None, -1, 10, None, 1, 0, 999], "id": [4, 3, 3, 3, 2, 1, None]},
            id="desc asc, last last",
        ),
        pytest.param(
            [True, False],
            [False, True],
            {"a": [None, None, -1, 10, 1, 0, 999], "id": [4, 3, 3, 3, 2, 1, None]},
            id="desc asc, last first",
        ),
        pytest.param(
            [True, False],
            [True, False],
            {"a": [999, None, -1, 10, None, 1, 0], "id": [None, 4, 3, 3, 3, 2, 1]},
            id="desc asc, first last",
        ),
        pytest.param(
            [True, False],
            [True, True],
            {"a": [999, None, None, -1, 10, 1, 0], "id": [None, 4, 3, 3, 3, 2, 1]},
            id="desc asc, first first",
        ),
        pytest.param(
            [False, True],
            [False, False],
            {"a": [0, 1, 10, -1, None, None, 999], "id": [1, 2, 3, 3, 3, 4, None]},
            id="asc desc, last last",
        ),
        pytest.param(
            [False, True],
            [False, True],
            {"a": [0, 1, None, 10, -1, None, 999], "id": [1, 2, 3, 3, 3, 4, None]},
            id="asc desc, last first",
        ),
        pytest.param(
            [False, True],
            [True, False],
            {"a": [999, 0, 1, 10, -1, None, None], "id": [None, 1, 2, 3, 3, 3, 4]},
            id="asc desc, first last",
        ),
        pytest.param(
            [False, True],
            [True, True],
            {"a": [999, 0, 1, None, 10, -1, None], "id": [None, 1, 2, 3, 3, 3, 4]},
            id="asc desc, first first",
        ),
        pytest.param(
            [True, True],
            [False, False],
            {"a": [None, 10, -1, None, 1, 0, 999], "id": [4, 3, 3, 3, 2, 1, None]},
            id="desc desc, last last",
        ),
        pytest.param(
            [True, True],
            [False, True],
            {"a": [None, None, 10, -1, 1, 0, 999], "id": [4, 3, 3, 3, 2, 1, None]},
            id="desc desc, last first",
        ),
        pytest.param(
            [True, True],
            [True, False],
            {"a": [999, None, 10, -1, None, 1, 0], "id": [None, 4, 3, 3, 3, 2, 1]},
            id="desc desc, first last",
        ),
        pytest.param(
            [True, True],
            [True, True],
            {"a": [999, None, None, 10, -1, 1, 0], "id": [None, 4, 3, 3, 3, 2, 1]},
            id="desc desc, first first",
        ),
    ],
)
def test_multi_column_sort_combinations(desc, nulls_first, expected):
    df = daft.from_pydict(
        {
            "a": [0, 1, -1, 10, None, 999, None],
            "id": [1, 2, 3, 3, 4, None, 3],
        }
    )
    result = df.sort(by=["id", "a"], desc=desc, nulls_first=nulls_first)
    assert result.to_pydict() == expected


@pytest.mark.parametrize(
    "sort_keys,desc,nulls_first,expected",
    [
        pytest.param(
            "a",
            False,
            None,
            {"a": [-100, -1, 0, 1], "id": [3, None, 1, 2]},
            id="a, asc",
        ),
        pytest.param(
            "a",
            False,
            True,
            {"a": [None, -100, -1, 0], "id": [4, 3, None, 1]},
            id="a, asc, nulls first",
        ),
        pytest.param(
            ["a", "id"],
            [False, True],
            None,
            {"a": [-100, -1, 0, 1], "id": [3, None, 1, 2]},
            id="a id, asc desc",
        ),
        pytest.param(
            ["id", "a"],
            [False, True],
            [True, True],
            {"a": [999, -1, 0, 1], "id": [None, None, 1, 2]},
            id="id a, asc desc, nulls first first",
        ),
    ],
)
def test_top_k_basic(sort_keys, desc, nulls_first, expected):
    df = daft.from_pydict(
        {
            "a": [0, 1, -1, 10, None, 999, -100],
            "id": [1, 2, None, 3, 4, None, 3],
        }
    )
    result = df.sort(by=sort_keys, desc=desc, nulls_first=nulls_first).limit(4)
    assert result.to_pydict() == expected


@pytest.mark.parametrize(
    "sort_keys,desc,nulls_first,expected",
    [
        pytest.param(
            ["id", "a"],
            [False, True],
            [True, True],
            {"a": [999, -1, 0, 1, None, 10], "id": [None, None, 1, 2, 3, 3]},
            id="id a, asc desc, nulls first first",
        ),
        pytest.param(
            ["id", "a"],
            [True, True],
            [True, False],
            {"a": [999, -1, None, 10, None, 1], "id": [None, None, 4, 3, 3, 2]},
            id="id a, asc desc, nulls first first",
        ),
    ],
)
def test_top_k_second_level_sorting(sort_keys, desc, nulls_first, expected):
    df = daft.from_pydict(
        {
            "a": [0, 1, -1, 10, None, 999, None],
            "id": [1, 2, None, 3, 4, None, 3],
        }
    )
    result = df.sort(by=sort_keys, desc=desc, nulls_first=nulls_first).limit(6)
    assert result.to_pydict() == expected
