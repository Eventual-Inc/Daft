from __future__ import annotations

import math

import pyarrow as pa
import pytest

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
def test_single_float_col_sort(make_df, desc: bool, n_partitions: int):
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
def test_multi_float_col_sort(make_df, n_partitions: int):
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
def test_single_string_col_sort(make_df, desc: bool, n_partitions: int):
    df = make_df({"A": ["0", None, "1", "", "01"]}, repartition=n_partitions)
    df = df.sort("A", desc=desc)
    sorted_data = df.to_pydict()

    expected = ["", "0", "01", "1", None]
    if desc:
        expected = list(reversed(expected))

    assert sorted_data["A"] == expected


@pytest.mark.parametrize("desc", [True, False])
@pytest.mark.parametrize("n_partitions", [1, 3, 4])
def test_single_bool_col_sort(make_df, desc: bool, n_partitions: int):
    df = make_df({"A": [True, None, False, True, False]}, repartition=n_partitions)
    df = df.sort("A", desc=desc)
    sorted_data = df.to_pydict()

    expected = [False, False, True, True, None]
    if desc:
        expected = list(reversed(expected))

    assert sorted_data["A"] == expected


@pytest.mark.parametrize("n_partitions", [1, 3, 4])
def test_multi_bool_col_sort(make_df, n_partitions: int):
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
def test_int_sort_with_nulls(make_df, repartition_nparts):
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
def test_str_sort_with_nulls(make_df, repartition_nparts):
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
def test_sort_with_nulls_multikey(make_df, repartition_nparts):
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
def test_sort_with_all_nulls(make_df, repartition_nparts):
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
def test_sort_with_empty(make_df, repartition_nparts):
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
