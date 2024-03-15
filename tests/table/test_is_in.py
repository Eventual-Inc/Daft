from __future__ import annotations

import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from daft import col
from daft.series import Series
from daft.table import MicroPartition


class CustomClass:
    def __init__(self, x):
        self.x = x

    def __eq__(self, other):
        return self.x == other.x

    def __hash__(self):
        return hash(self.x)


class CustomClassWithoutHash:
    def __init__(self, x):
        self.x = x

    def __eq__(self, other):
        return self.x == other.x


@pytest.mark.parametrize(
    "input,items,expected",
    [
        pytest.param([None, None], [None], [None, None], id="NullColumn"),
        pytest.param([True, False], [False], [False, True], id="BooleanColumn"),
        pytest.param(["a", "b", "c", "d"], ["a", "b"], [True, True, False, False], id="StringColumn"),
        pytest.param([b"a", b"b", b"c", b"d"], [b"a", b"b"], [True, True, False, False], id="BinaryColumn"),
        pytest.param([-1, 2, 3, 4], [-1, 2], [True, True, False, False], id="IntColumn"),
        pytest.param([-1.0, 2.0, 3.0, 4.0], [-1.0, 2.0], [True, True, False, False], id="FloatColumn"),
        pytest.param(
            [datetime.date.today(), datetime.date.today() - datetime.timedelta(days=1)],
            [datetime.date.today()],
            [True, False],
            id="DateColumn",
        ),
        pytest.param(
            [datetime.datetime(2022, 1, 1), datetime.datetime(2023, 1, 1)],
            [datetime.datetime(2022, 1, 1)],
            [True, False],
            id="TimestampColumn",
        ),
        pytest.param([CustomClass(1), CustomClass(2)], [CustomClass(1)], [True, False], id="ObjectColumn"),
        pytest.param(
            [CustomClassWithoutHash(1), CustomClassWithoutHash(2)],
            [CustomClassWithoutHash(1)],
            [True, False],
            id="ObjectWithoutHashColumn",
        ),
    ],
)
def test_table_expr_is_in_same_types(input, items, expected) -> None:
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").is_in(items)])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


@pytest.mark.parametrize(
    "input,items,expected",
    [
        # Int
        pytest.param([-1, 2, 3, 4], ["-1", "2"], None, id="IntWithString"),
        pytest.param([1, 2, 3, 4], [1.0, 2.0], [True, True, False, False], id="IntWithFloat"),
        pytest.param([0, 1, 2, 3], [True], [False, True, False, False], id="IntWithBool"),
        # Float
        pytest.param([-1.0, 2.0, 3.0, 4.0], ["-1.0", "2.0"], None, id="FloatWithString"),
        pytest.param([1.0, 2.0, 3.0, 4.0], [1, 2], [True, True, False, False], id="FloatWithInt"),
        pytest.param([0.0, 1.0, 2.0, 3.0], [True], [False, True, False, False], id="FloatWithBool"),
        # String
        pytest.param(["1", "2", "3", "4"], [1, 2], None, id="StringWithInt"),
        pytest.param(["1.0", "2.0", "3.0", "4.0"], [1.0, 2.0], None, id="StringWithFloat"),
        # Bool
        pytest.param([True, False, None], [1, 0], [True, True, None], id="BoolWithInt"),
        pytest.param([True, False, None], [1.0], [True, False, None], id="BoolWithFloat"),
        # Date
        pytest.param(
            [datetime.date.today(), datetime.date.today() - datetime.timedelta(days=1)],
            [datetime.datetime.today()],
            [True, False],
            id="DateWithTimestamp",
        ),
        # Timestamp
        pytest.param(
            [datetime.datetime(2022, 1, 1), datetime.datetime(2023, 1, 1)],
            [datetime.date(2022, 1, 1)],
            [True, False],
            id="TimestampWithDate",
        ),
    ],
)
def test_table_expr_is_in_different_types_castable(input, items, expected) -> None:
    daft_table = MicroPartition.from_pydict({"input": input})

    if expected is None:
        with pytest.raises(ValueError, match="Cannot perform comparison on types:"):
            daft_table = daft_table.eval_expression_list([col("input").is_in(items)])
    else:
        daft_table = daft_table.eval_expression_list([col("input").is_in(items)])
        pydict = daft_table.to_pydict()
        assert pydict["input"] == expected


@pytest.mark.parametrize(
    "input,items,expected",
    [
        pytest.param([None, None, None], [None], [None, None, None], id="NullColumn"),
        pytest.param([True, False, None], [None], [False, False, None], id="BooleanColumn"),
        pytest.param(["a", "b", None], [None], [False, False, None], id="StringColumn"),
        pytest.param([b"a", b"b", None], [None], [False, False, None], id="BinaryColumn"),
        pytest.param([1, 2, None], [None], [False, False, None], id="IntColumn"),
        pytest.param([1.0, 2.0, None], [None], [False, False, None], id="FloatColumn"),
    ],
)
def test_table_expr_is_in_items_is_none(input, items, expected) -> None:
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").is_in(items)])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


@pytest.mark.parametrize(
    "input,items,expected",
    [
        pytest.param([1, 2, 3, 4], np.array([1, 2]), [True, True, False, False], id="NumpyArray"),
        pytest.param([1, 2, 3, 4], pa.array([1, 2], type=pa.int8()), [True, True, False, False], id="PyArrowArray"),
        pytest.param([1, 2, 3, 4], pd.Series([1, 2]), [True, True, False, False], id="PandasSeries"),
        pytest.param([1, 2, 3, 4], Series.from_pylist([1, 2]), [True, True, False, False], id="DaftSeries"),
    ],
)
def test_table_expr_is_in_different_input_types(input, items, expected) -> None:
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").is_in(items)])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


def test_table_expr_is_in_with_another_df_column() -> None:
    daft_table = MicroPartition.from_pydict({"input": [1, 2, 3, 4], "items": [3, 4, 5, 6]})
    daft_table = daft_table.eval_expression_list([col("input").is_in(col("items"))])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == [False, False, True, True]


def test_table_expr_is_in_empty_items() -> None:
    daft_table = MicroPartition.from_pydict({"input": [1, 2, 3, 4]})
    daft_table = daft_table.eval_expression_list([col("input").is_in([])])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == [False, False, False, False]


def test_table_expr_is_in_items_invalid_input() -> None:
    daft_table = MicroPartition.from_pydict({"input": [1, 2, 3, 4]})

    with pytest.raises(ValueError, match="Creating a Series from data of type"):
        daft_table.eval_expression_list([col("input").is_in(1)])
