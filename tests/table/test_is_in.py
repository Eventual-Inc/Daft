from __future__ import annotations

import datetime

import pytest

from daft import col
from daft.table import MicroPartition


@pytest.mark.parametrize(
    "input,items,expected",
    [
        pytest.param([None, None], [None], [True, True], id="NullColumn"),
        pytest.param([True, False, None], [True], [True, False, False], id="BooleanColumn"),
        pytest.param(["a", "b", "c", "d"], ["a", "b"], [True, True, False, False], id="StringColumn"),
        pytest.param([b"a", b"b", b"c", b"d"], [b"a", b"b"], [True, True, False, False], id="BinaryColumn"),
        pytest.param([-1, 2, 3, 4], [-1, 2], [True, True, False, False], id="IntColumn"),
        pytest.param([-1.0, 2.0, 3.0, 4.0], [-1.0, 2.0], [True, True, False, False], id="FloatColumn"),
    ],
)
def test_table_expr_is_in(input, items, expected) -> None:
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


@pytest.mark.parametrize(
    "input,items,expected",
    [
        # Int
        pytest.param([-1, 2, 3, 4], ["-1", "2"], [True, True, False, False], id="IntWithString"),
        pytest.param([1, 2, 3, 4], [1.0, 2.0], [True, True, False, False], id="IntWithFloat"),
        pytest.param([0, 1, 2, 3], [True], [False, True, True, True], id="IntWithBool"),
        pytest.param([1, 2, 3, 4], [b"1", b"2"], [True, True, False, False], id="IntWithBinary"),
        # Float
        pytest.param([-1.0, 2.0, 3.0, 4.0], ["-1.0", "2.0"], [True, True, False, False], id="FloatWithString"),
        pytest.param([1.0, 2.0, 3.0, 4.0], [1, 2], [True, True, False, False], id="FloatWithInt"),
        pytest.param([0.0, 1.0, 2.0, 3.0], [True], [False, True, True, True], id="FloatWithBool"),
        pytest.param([1.0, 2.0, 3.0, 4.0], [b"1.0", b"2.0"], [True, True, False, False], id="FloatWithBinary"),
        # String
        pytest.param(["1", "2", "3", "4"], [1, 2], [True, True, False, False], id="StringWithInt"),
        pytest.param(["1.0", "2.0", "3.0", "4.0"], [1.0, 2.0], [True, True, False, False], id="StringWithFloat"),
        pytest.param(["True", "False", ""], [True], [False, False, False], id="StringWithBool"),
        pytest.param(["a", "b", "c", "d"], [b"a", b"b"], [True, True, False, False], id="StringWithBinary"),
        # Binary
        pytest.param([b"a", b"b", b"c", b"d"], ["a", "b"], [True, True, False, False], id="BinaryWithString"),
        pytest.param([b"1.0", b"2.0", b"3.0", b"4.0"], [1.0, 2.0], [True, True, False, False], id="BinaryWithFloat"),
        pytest.param([b"True", b"False", b""], [True], [False, False, False], id="BinaryWithBool"),
        pytest.param([b"1", b"2", b"3", b"4"], [1, 2], [True, True, False, False], id="BinaryWithInt"),
        # Bool
        pytest.param([True, False, None], [1, 0], [True, True, False], id="BoolWithInt"),
        pytest.param([True, False, None], [1.0], [True, False, False], id="BoolWithFloat"),
        pytest.param([True, False, None], ["True"], [False, False, False], id="BoolWithString"),
        pytest.param([True, False, None], [b"True"], [False, False, False], id="BoolWithBinary"),
    ],
)
def test_table_expr_is_in_different_types(input, items, expected) -> None:
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").is_in(items)])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


@pytest.mark.parametrize(
    "input,items",
    [
        pytest.param([[1, 2, 3], [4, 5, 6]], [[1, 2, 3]], id="NestedListColumn"),
        pytest.param([{"a": 1}, {"b": 1}], [{"a": 1}], id="NestedStructColumn"),
        # TODO: Add support for these types
        # Currently list of datetime/timestamp are parsed as LiteralValue::List(Python) instead of LiteralValue::List(Date/Timestamp)
        # because the default _lit function does not support these types. There are separate _date_lit and _timestamp_lit functions but these are for
        # single values only. Need to implement a _datelist_lit / _timestamplist_lit function that supports these types.
        pytest.param(
            [datetime.date.today(), datetime.date.today() - datetime.timedelta(days=1)],
            [datetime.date.today()],
            id="DateColumn",
        ),
        pytest.param(
            [datetime.datetime.now(), datetime.datetime.now() - datetime.timedelta(days=1)],
            [datetime.datetime.now()],
            id="TimestampColumn",
        ),
    ],
)
def test_table_expr_is_in_unsupported_types(input, items) -> None:
    daft_table = MicroPartition.from_pydict({"input": input})

    with pytest.raises(ValueError, match="are not supported for `is_in`"):
        daft_table.eval_expression_list([col("input").is_in(items)])


def test_table_expr_is_in_empty_items() -> None:
    daft_table = MicroPartition.from_pydict({"input": [1, 2, 3, 4]})
    daft_table = daft_table.eval_expression_list([col("input").is_in([])])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == [False, False, False, False]


def test_table_expr_is_in_items_different_types() -> None:
    daft_table = MicroPartition.from_pydict({"input": [1, 2, 3, 4]})

    with pytest.raises(ValueError, match="all items in list must be the same type"):
        daft_table.eval_expression_list([col("input").is_in([1, 2.0])])


def test_table_expr_is_in_items_is_not_list() -> None:
    daft_table = MicroPartition.from_pydict({"input": [1, 2, 3, 4]})

    with pytest.raises(TypeError, match="is_in expects a list"):
        daft_table.eval_expression_list([col("input").is_in(1)])


def test_table_expr_is_in_empty_items() -> None:
    daft_table = MicroPartition.from_pydict({"input": [1, 2, 3, 4]})

    with pytest.raises(TypeError, match="missing 1 required positional argument"):
        daft_table.eval_expression_list([col("input").is_in()])
