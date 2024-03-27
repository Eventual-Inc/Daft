from __future__ import annotations

import itertools
import math
import operator as ops

import pyarrow as pa
import pytest

from daft import DataType, col
from daft.table import MicroPartition
from tests.table import daft_numeric_types


def test_table_eval_expressions() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = MicroPartition.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") + col("b"), col("b") * 2]
    new_table = daft_table.eval_expression_list(exprs)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [6, 8, 10, 12]
    assert result["b"] == [10, 12, 14, 16]


def test_table_eval_expressions_conflict() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = MicroPartition.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") + col("b"), col("a") * 2]

    with pytest.raises(ValueError, match="Duplicate name"):
        daft_table.eval_expression_list(exprs)


@pytest.mark.parametrize(
    "input,expr,expected",
    [
        pytest.param([True, False, None], ~col("input"), [False, True, None], id="BooleanColumn"),
        pytest.param(["apple", None, "banana"], ~(col("input") != "banana"), [False, None, True], id="BooleanExpr"),
        pytest.param([], ~(col("input").cast(DataType.bool())), [], id="EmptyColumn"),
    ],
)
def test_table_expr_not(input, expr, expected) -> None:
    """Test logical not expression."""
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([expr])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


def test_table_expr_not_wrong() -> None:
    daft_table = MicroPartition.from_pydict({"input": [None, 0, 1]})

    with pytest.raises(ValueError):
        daft_table = daft_table.eval_expression_list([~col("input")])


@pytest.mark.parametrize(
    "input,expected",
    [
        pytest.param([True, False, None], [False, False, True], id="BooleanColumn"),
        pytest.param(["a", "b", "c"], [False, False, False], id="StringColumn"),
        pytest.param([None, None], [True, True], id="NullColumn"),
        pytest.param([], [], id="EmptyColumn"),
        pytest.param([[1, 2, 3], [4, 5, 6], None], [False, False, True], id="NestedListColumn"),
        pytest.param([{"a": 1}, {"b": 1}, None], [False, False, True], id="NestedStructColumn"),
    ],
)
def test_table_expr_is_null(input, expected) -> None:
    """Test is_null expression."""
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").is_null()])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        pytest.param([True, False, None], [True, True, False], id="BooleanColumn"),
        pytest.param(["a", "b", "c"], [True, True, True], id="StringColumn"),
        pytest.param([None, None], [False, False], id="NullColumn"),
        pytest.param([], [], id="EmptyColumn"),
        pytest.param([[1, 2, 3], [4, 5, 6], None], [True, True, False], id="NestedListColumn"),
        pytest.param([{"a": 1}, {"b": 1}, None], [True, True, False], id="NestedStructColumn"),
    ],
)
def test_table_expr_not_null(input, expected) -> None:
    """Test not_null expression."""
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").not_null()])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


OPS = [ops.add, ops.sub, ops.mul, ops.truediv, ops.mod, ops.lt, ops.le, ops.eq, ops.ne, ops.ge, ops.gt]


@pytest.mark.parametrize("data_dtype, op", itertools.product(daft_numeric_types, OPS))
def test_table_numeric_expressions(data_dtype, op) -> None:
    a, b = [5, 6, 7, 8], [1, 2, 3, 4]
    pa_table = pa.Table.from_pydict({"a": a, "b": b})

    daft_table = MicroPartition.from_arrow(pa_table)
    daft_table = daft_table.eval_expression_list(
        [op(col("a").cast(data_dtype), col("b").cast(data_dtype)).alias("result")]
    )

    assert len(daft_table) == 4
    assert daft_table.column_names() == ["result"]
    pyresult = [op(l, r) for l, r in zip(a, b)]
    assert daft_table.get_column("result").to_pylist() == pyresult


@pytest.mark.parametrize("data_dtype, op", itertools.product(daft_numeric_types, OPS))
def test_table_numeric_expressions_with_nulls(data_dtype, op) -> None:
    a, b = [5, 6, None, 8, None], [1, 2, 3, None, None]
    pa_table = pa.Table.from_pydict({"a": a, "b": b})

    daft_table = MicroPartition.from_arrow(pa_table)
    daft_table = daft_table.eval_expression_list(
        [op(col("a").cast(data_dtype), col("b").cast(data_dtype)).alias("result")]
    )

    assert len(daft_table) == 5
    assert daft_table.column_names() == ["result"]
    pyresult = [op(l, r) for l, r in zip(a[:2], b[:2])]
    assert daft_table.get_column("result").to_pylist()[:2] == pyresult

    assert daft_table.get_column("result").to_pylist()[2:] == [None, None, None]


def test_table_numeric_abs() -> None:
    table = MicroPartition.from_pydict({"a": [None, -1.0, 0, 2, 3, None], "b": [-1, -2, 3, 4, None, None]})

    abs_table = table.eval_expression_list([abs(col("a")), col("b").abs()])

    assert [abs(v) if v is not None else v for v in table.get_column("a").to_pylist()] == abs_table.get_column(
        "a"
    ).to_pylist()
    assert [abs(v) if v is not None else v for v in table.get_column("b").to_pylist()] == abs_table.get_column(
        "b"
    ).to_pylist()


def test_table_abs_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to abs to be numeric"):
        table.eval_expression_list([abs(col("a"))])


def test_table_numeric_ceil() -> None:
    table = MicroPartition.from_pydict(
        {"a": [None, -1.0, -0.5, 0, 0.5, 2, None], "b": [-1.7, -1.5, -1.3, 0.3, 0.7, None, None]}
    )

    ceil_table = table.eval_expression_list([col("a").ceil(), col("b").ceil()])

    assert [math.ceil(v) if v is not None else v for v in table.get_column("a").to_pylist()] == ceil_table.get_column(
        "a"
    ).to_pylist()
    assert [math.ceil(v) if v is not None else v for v in table.get_column("b").to_pylist()] == ceil_table.get_column(
        "b"
    ).to_pylist()


def test_table_ceil_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to ceil to be numeric"):
        table.eval_expression_list([col("a").ceil()])


def test_table_numeric_floor() -> None:
    table = MicroPartition.from_pydict(
        {"a": [None, -1.0, -0.5, 0.0, 0.5, 2, None], "b": [-1.7, -1.5, -1.3, 0.3, 0.7, None, None]}
    )

    floor_table = table.eval_expression_list([col("a").floor(), col("b").floor()])

    assert [math.floor(v) if v is not None else v for v in table.get_column("a").to_pylist()] == floor_table.get_column(
        "a"
    ).to_pylist()
    assert [math.floor(v) if v is not None else v for v in table.get_column("b").to_pylist()] == floor_table.get_column(
        "b"
    ).to_pylist()


def test_table_floor_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to floor to be numeric"):
        table.eval_expression_list([col("a").floor()])


def test_table_numeric_sign() -> None:
    table = MicroPartition.from_pydict(
        {"a": [None, -1, -5, 0, 5, 2, None], "b": [-1.7, -1.5, -1.3, 0.3, 0.7, None, None]}
    )
    my_schema = pa.schema([pa.field("uint8", pa.uint8())])
    table_Unsign = MicroPartition.from_arrow(pa.Table.from_arrays([pa.array([None, 0, 1, 2, 3])], schema=my_schema))

    sign_table = table.eval_expression_list([col("a").sign(), col("b").sign()])
    unsign_sign_table = table_Unsign.eval_expression_list([col("uint8").sign()])

    def checkSign(val):
        if val < 0:
            return -1
        if val > 0:
            return 1
        return 0

    assert [checkSign(v) if v is not None else v for v in table.get_column("a").to_pylist()] == sign_table.get_column(
        "a"
    ).to_pylist()
    assert [checkSign(v) if v is not None else v for v in table.get_column("b").to_pylist()] == sign_table.get_column(
        "b"
    ).to_pylist()
    assert [
        checkSign(v) if v is not None else v for v in table_Unsign.get_column("uint8").to_pylist()
    ] == unsign_sign_table.get_column("uint8").to_pylist()


def test_table_sign_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to sign to be numeric"):
        table.eval_expression_list([col("a").sign()])


def test_table_numeric_round() -> None:
    from decimal import ROUND_HALF_UP, Decimal

    table = MicroPartition.from_pydict(
        {"a": [None, -1, -5, 0, 5, 2, None], "b": [-1.765, -1.565, -1.321, 0.399, 0.781, None, None]}
    )
    round_table = table.eval_expression_list([col("a").round(None), col("b").round(2)])
    assert [
        Decimal(v).to_integral_value(rounding=ROUND_HALF_UP) if v is not None else v
        for v in table.get_column("a").to_pylist()
    ] == round_table.get_column("a").to_pylist()
    assert [
        float(Decimal(str(v)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)) if v is not None else v
        for v in table.get_column("b").to_pylist()
    ] == round_table.get_column("b").to_pylist()


def test_table_round_bad_input() -> None:
    table = MicroPartition.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to round to be numeric"):
        table.eval_expression_list([col("a").round()])

    table = MicroPartition.from_pydict({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="decimal can not be negative: -2"):
        table.eval_expression_list([col("a").round(-2)])
