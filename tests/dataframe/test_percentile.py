from __future__ import annotations

import pytest

import daft
from daft import DataType, col


def test_percentile_global():
    df = daft.from_pydict({"values": [1.0, 2.0, 3.0, 4.0]})

    actual = df.agg(col("values").percentile(0.5).alias("p50")).collect().to_pydict()
    assert actual == {"p50": [2.5]}


def test_percentile_groupby():
    df = daft.from_pydict(
        {
            "id": [1, 1, 1, 2, 2],
            "values": [1.0, 2.0, 10.0, 4.0, 6.0],
        }
    )

    actual = df.groupby("id").agg(col("values").percentile(0.5).alias("p50")).sort("id").collect().to_pydict()
    assert actual == {"id": [1, 2], "p50": [2.0, 5.0]}


def test_percentile_50_is_median():
    df = daft.from_pydict({"values": [1.0, None, 2.0, 10.0]})

    actual = (
        df.agg(
            [
                col("values").percentile(0.5).alias("p50"),
                col("values").median().alias("median"),
            ]
        )
        .collect()
        .to_pydict()
    )
    assert actual == {"p50": [2.0], "median": [2.0]}


def test_percentile_all_nulls():
    df = daft.from_pydict({"values": [None, None]})

    actual = (
        df.agg(
            [
                col("values").cast(DataType.float64()).percentile(0.5).alias("p50"),
                col("values").cast(DataType.float64()).median().alias("median"),
            ]
        )
        .collect()
        .to_pydict()
    )
    assert actual == {"p50": [None], "median": [None]}


def test_percentile_empty_input():
    df = daft.from_pydict({"values": [1.0, 2.0]}).limit(0)

    actual = (
        df.agg(
            [
                col("values").percentile(0.5).alias("p50"),
                col("values").median().alias("median"),
            ]
        )
        .collect()
        .to_pydict()
    )
    assert actual == {"p50": [None], "median": [None]}


def test_percentile_sql_invalid_percentage_raises():
    df = daft.from_pydict({"values": [1.0, 2.0, 3.0]})

    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT percentile(values, 1.5) FROM df", df=df).collect()

    assert "between 0.0 and 1.0" in str(excinfo.value)


def test_percentile_sql_integer_percentage_raises():
    df = daft.from_pydict({"values": [1.0, 2.0, 3.0]})

    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT percentile(values, 1) FROM df", df=df).collect()

    assert "float literal" in str(excinfo.value)
