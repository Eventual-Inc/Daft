from __future__ import annotations

import pytest

from daft import col
from daft.datatype import DataType


@pytest.mark.parametrize("n_partitions", [2])
def test_unpivot(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "id": ["x", "y", "z"],
            "a": [1, 3, 5],
            "b": [2, 4, 6],
        },
        repartition=n_partitions,
    )

    df = df.unpivot("id", ["a", "b"])
    df = df.sort(["id", "variable"])
    df = df.collect()

    expected = {
        "id": ["x", "x", "y", "y", "z", "z"],
        "variable": ["a", "b", "a", "b", "a", "b"],
        "value": [1, 2, 3, 4, 5, 6],
    }

    assert df.to_pydict() == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_unpivot_no_values(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "id": ["x", "y", "z"],
            "a": [1, 3, 5],
            "b": [2, 4, 6],
        },
        repartition=n_partitions,
    )

    df = df.unpivot("id")
    df = df.sort(["id", "variable"])
    df = df.collect()

    expected = {
        "id": ["x", "x", "y", "y", "z", "z"],
        "variable": ["a", "b", "a", "b", "a", "b"],
        "value": [1, 2, 3, 4, 5, 6],
    }

    assert df.to_pydict() == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_unpivot_different_types(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "id": ["x", "y", "z"],
            "a": [1, 3, 5],
            "b": [1.1, 3.3, 5.5],
        },
        repartition=n_partitions,
    )

    df = df.unpivot("id", ["a", "b"])
    df = df.sort(["id", "variable"])
    df = df.collect()

    expected = {
        "id": ["x", "x", "y", "y", "z", "z"],
        "variable": ["a", "b", "a", "b", "a", "b"],
        "value": [1, 1.1, 3, 3.3, 5, 5.5],
    }

    assert df.to_pydict() == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_unpivot_incompatible_types(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "id": ["x", "y", "z"],
            "a": [[1], [2, 3], [4, 5, 6]],
            "b": [7, 8, 9],
        },
        repartition=n_partitions,
    )

    with pytest.raises(ValueError):
        df = df.unpivot("id", ["a", "b"])


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_unpivot_nulls(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "id": ["x", "y", "z"],
            "a": [1, None, 5],
            "b": [2, 4, None],
        },
        repartition=n_partitions,
    )

    df = df.unpivot("id", ["a", "b"])
    df = df.sort(["id", "variable"])
    df = df.collect()

    expected = {
        "id": ["x", "x", "y", "y", "z", "z"],
        "variable": ["a", "b", "a", "b", "a", "b"],
        "value": [1, 2, None, 4, 5, None],
    }

    assert df.to_pydict() == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_unpivot_null_column(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "id": ["x", "y", "z"],
            "a": [1, 3, 5],
            "b": [None, None, None],
        },
        repartition=n_partitions,
    )

    df = df.unpivot("id", ["a", "b"])
    df = df.sort(["id", "variable"])
    df = df.collect()

    expected = {
        "id": ["x", "x", "y", "y", "z", "z"],
        "variable": ["a", "b", "a", "b", "a", "b"],
        "value": [1, None, 3, None, 5, None],
    }

    assert df.to_pydict() == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_unpivot_multiple_ids(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "id1": ["x", "y", "z"],
            "id2": [7, 8, 9],
            "a": [1, 3, 5],
            "b": [2, 4, 6],
        },
        repartition=n_partitions,
    )

    df = df.unpivot(["id1", "id2"], ["a", "b"])
    df = df.sort(["id1", "id2", "variable"])
    df = df.collect()

    expected = {
        "id1": ["x", "x", "y", "y", "z", "z"],
        "id2": [7, 7, 8, 8, 9, 9],
        "variable": ["a", "b", "a", "b", "a", "b"],
        "value": [1, 2, 3, 4, 5, 6],
    }

    assert df.to_pydict() == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_unpivot_no_ids(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "a": [1, 3, 5],
            "b": [2, 4, 6],
        },
        repartition=n_partitions,
    )

    df = df.unpivot([])
    df = df.sort("value")
    df = df.collect()

    expected = {
        "variable": ["a", "b", "a", "b", "a", "b"],
        "value": [1, 2, 3, 4, 5, 6],
    }

    assert df.to_pydict() == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_unpivot_expr(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "id": ["x", "y", "z"],
            "a": [1, 3, 5],
            "b": [2, 4, 6],
        },
        repartition=n_partitions,
    )

    df = df.unpivot("id", ["a", "b", (col("a") + col("b")).alias("a_plus_b")])
    df = df.sort(["id", "variable"])
    df = df.collect()

    expected = {
        "id": ["x", "x", "x", "y", "y", "y", "z", "z", "z"],
        "variable": ["a", "a_plus_b", "b", "a", "a_plus_b", "b", "a", "a_plus_b", "b"],
        "value": [1, 3, 2, 3, 7, 4, 5, 11, 6],
    }

    assert df.to_pydict() == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4])
def test_unpivot_followed_by_projection(make_df, n_partitions, with_morsel_size):
    df = make_df(
        {
            "year": [2020, 2021, 2022],
            "id": [1, 2, 3],
            "Jan": [10, 30, 50],
            "Feb": [20, 40, 60],
        },
        repartition=n_partitions,
    )

    df = df.unpivot("year", ["Jan", "Feb"], variable_name="month", value_name="inventory")
    df = df.with_column("inventory2", df["inventory"])
    df = df.sort(["year", "month", "inventory"])

    expected = {
        "year": [2020, 2020, 2021, 2021, 2022, 2022],
        "month": ["Feb", "Jan", "Feb", "Jan", "Feb", "Jan"],
        "inventory": [20, 10, 40, 30, 60, 50],
        "inventory2": [20, 10, 40, 30, 60, 50],
    }
    assert df.to_pydict() == expected


def test_unpivot_empty(make_df):
    df = make_df(
        {
            "id": [],
            "a": [],
            "b": [],
        }
    )

    df = df.unpivot("id", ["a", "b"])
    df = df.collect()

    expected = {
        "id": [],
        "variable": [],
        "value": [],
    }

    assert df.to_pydict() == expected
    assert df.schema()["variable"].dtype == DataType.string()


def test_unpivot_empty_partition(make_df):
    df = make_df(
        {
            "id": ["x", "y", "z"],
            "a": [1, 3, 5],
            "b": [2, 4, 6],
        }
    )

    df = df.into_partitions(4)
    df = df.unpivot("id", ["a", "b"])
    df = df.sort(["id", "variable"])
    df = df.collect()

    expected = {
        "id": ["x", "x", "y", "y", "z", "z"],
        "variable": ["a", "b", "a", "b", "a", "b"],
        "value": [1, 2, 3, 4, 5, 6],
    }

    assert df.to_pydict() == expected
