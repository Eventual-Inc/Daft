from __future__ import annotations

import datetime

import pytest

from daft import col
from daft.table import MicroPartition


class CustomClass:
    def __init__(self, x):
        self.x = x

    def __eq__(self, other):
        return self.x == other.x


@pytest.mark.parametrize(
    "input,items,expected",
    [
        pytest.param([None, None], [None], [True, True], id="NullColumn"),
        pytest.param([True, False, None], [True], [True, False, False], id="BooleanColumn"),
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
        pytest.param([-1, 2, 3, 4], ["-1", "2"], [True, True, False, False], id="IntWithString"),
        pytest.param([1, 2, 3, 4], [1.0, 2.0], [True, True, False, False], id="IntWithFloat"),
        pytest.param([0, 1, 2, 3], [True], [False, True, False, False], id="IntWithBool"),
        # Float
        pytest.param([-1.0, 2.0, 3.0, 4.0], ["-1.0", "2.0"], [True, True, False, False], id="FloatWithString"),
        pytest.param([1.0, 2.0, 3.0, 4.0], [1, 2], [True, True, False, False], id="FloatWithInt"),
        pytest.param([0.0, 1.0, 2.0, 3.0], [True], [False, True, False, False], id="FloatWithBool"),
        # String
        pytest.param(["1", "2", "3", "4"], [1, 2], [True, True, False, False], id="StringWithInt"),
        pytest.param(["1.0", "2.0", "3.0", "4.0"], [1.0, 2.0], [True, True, False, False], id="StringWithFloat"),
        pytest.param(["True", "False", ""], [True], [False, False, False], id="StringWithBool"),
        # Binary
        pytest.param([b"True", b"False", b""], [True], [False, False, False], id="BinaryWithBool"),
        # Bool
        pytest.param([True, False, None], [1, 0], [True, True, False], id="BoolWithInt"),
        pytest.param([True, False, None], [1.0], [True, False, False], id="BoolWithFloat"),
        pytest.param([True, False, None], ["True"], [False, False, False], id="BoolWithString"),
        pytest.param([True, False, None], [b"True"], [False, False, False], id="BoolWithBinary"),
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
    daft_table = daft_table.eval_expression_list([col("input").is_in(items)])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


@pytest.mark.parametrize(
    "input,items,expected",
    [
        pytest.param([None, None, None], [None], [True, True, True], id="NullColumn"),
        pytest.param([True, False, None], [None], [False, False, True], id="BooleanColumn"),
        pytest.param(["a", "b", None], [None], [False, False, True], id="StringColumn"),
        pytest.param([b"a", b"b", None], [None], [False, False, True], id="BinaryColumn"),
        pytest.param([1, 2, None], [None], [False, False, True], id="IntColumn"),
        pytest.param([1.0, 2.0, None], [None], [False, False, True], id="FloatColumn"),
    ],
)
def test_table_expr_is_in_items_is_none(input, items, expected) -> None:
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").is_in(items)])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


def test_table_expr_is_in_empty_items() -> None:
    daft_table = MicroPartition.from_pydict({"input": [1, 2, 3, 4]})
    daft_table = daft_table.eval_expression_list([col("input").is_in([])])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == [False, False, False, False]


def test_table_expr_is_in_items_is_not_list_or_series() -> None:
    daft_table = MicroPartition.from_pydict({"input": [1, 2, 3, 4]})

    with pytest.raises(TypeError, match="expected a python list or Daft Series"):
        daft_table.eval_expression_list([col("input").is_in(1)])
