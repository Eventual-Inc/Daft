from __future__ import annotations

import pytest

import daft
from daft.datatype import DataType


@pytest.fixture(scope="function")
def df():
    return daft.from_pydict({"foo": [1, 2, 3]})


def test_num_partitions(df):
    assert df.num_partitions() == 1

    df2 = df.repartition(2)
    assert df2.num_partitions() == 2


def test_schema(df):
    fields = [f for f in df.schema()]
    assert len(fields) == 1
    [field] = fields
    assert field.name == "foo"
    assert field.dtype == DataType.int64()


def test_column_names(df):
    assert df.column_names == ["foo"]


def test_columns(df):
    assert len(df.columns) == 1
    [ex] = df.columns
    assert ex.name() == "foo"
