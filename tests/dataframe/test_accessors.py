from __future__ import annotations

import pytest

from daft import DataFrame
from daft.expressions import ColumnExpression
from daft.logical.field import Field
from daft.logical.logical_plan import LogicalPlan
from daft.types import ExpressionType


@pytest.fixture(scope="function")
def df():
    return DataFrame.from_pydict({"foo": [1, 2, 3]})


def test_get_plan(df):
    assert isinstance(df.plan(), LogicalPlan)


def test_num_partitions(df):
    assert df.num_partitions() == 1

    df2 = df.repartition(2)
    assert df2.num_partitions() == 2


def test_schema(df):
    assert [f for f in df.schema()] == [Field("foo", ExpressionType.integer())]


def test_column_names(df):
    assert df.column_names == ["foo"]


def test_columns(df):
    assert len(df.columns) == 1
    [ex] = df.columns
    assert isinstance(ex, ColumnExpression)
    assert ex.name() == "foo"
