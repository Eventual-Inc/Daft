from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataType, col
from daft.recordbatch import MicroPartition


def test_table_eval_expressions() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_recordbatch = MicroPartition.from_arrow(pa_table)
    assert len(daft_recordbatch) == 4
    assert daft_recordbatch.column_names() == ["a", "b"]

    exprs = [col("a") + col("b"), col("b") * 2]
    new_table = daft_recordbatch.eval_expression_list(exprs)
    assert len(daft_recordbatch) == 4
    assert daft_recordbatch.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [6, 8, 10, 12]
    assert result["b"] == [10, 12, 14, 16]


@pytest.mark.parametrize(
    "input,expr,expected",
    [
        pytest.param([True, False, None], ~col("input"), [False, True, None], id="BooleanColumn"),
        pytest.param(
            ["apple", None, "banana"],
            ~(col("input") != "banana"),
            [False, None, True],
            id="BooleanExpr",
        ),
        pytest.param([], ~(col("input").cast(DataType.bool())), [], id="EmptyColumn"),
    ],
)
def test_table_expr_not(input, expr, expected) -> None:
    """Test logical not expression."""
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([expr])
    pydict = daft_recordbatch.to_pydict()

    assert pydict["input"] == expected


def test_table_expr_not_wrong() -> None:
    daft_recordbatch = MicroPartition.from_pydict({"input": [None, 0, 1]})

    with pytest.raises(ValueError):
        daft_recordbatch = daft_recordbatch.eval_expression_list([~col("input")])


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
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").is_null()])
    pydict = daft_recordbatch.to_pydict()

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
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").not_null()])
    pydict = daft_recordbatch.to_pydict()

    assert pydict["input"] == expected
