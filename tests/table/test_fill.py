from __future__ import annotations

import datetime

import pytest

from daft.datatype import DataType
from daft.expressions.expressions import col
from daft.table.micropartition import MicroPartition


@pytest.mark.parametrize(
    "input,fill_value,expected",
    [
        pytest.param([None, None, None], "a", ["a", "a", "a"], id="NullColumn"),
        pytest.param([True, False, None], False, [True, False, False], id="BoolColumn"),
        pytest.param(["a", "b", None], "b", ["a", "b", "b"], id="StringColumn"),
        pytest.param([b"a", None, b"c"], b"b", [b"a", b"b", b"c"], id="BinaryColumn"),
        pytest.param([-1, None, 3], 0, [-1, 0, 3], id="IntColumn"),
        pytest.param([-1.0, None, 3.0], 0.0, [-1.0, 0.0, 3.0], id="FloatColumn"),
        pytest.param(
            [datetime.date.today(), None, datetime.date(2023, 1, 1)],
            datetime.date(2022, 1, 1),
            [
                datetime.date.today(),
                datetime.date(2022, 1, 1),
                datetime.date(2023, 1, 1),
            ],
        ),
        pytest.param(
            [datetime.datetime(2022, 1, 1), None, datetime.datetime(2023, 1, 1)],
            datetime.datetime(2022, 1, 1),
            [
                datetime.datetime(2022, 1, 1),
                datetime.datetime(2022, 1, 1),
                datetime.datetime(2023, 1, 1),
            ],
        ),
    ],
)
def test_table_expr_fill_null(input, fill_value, expected) -> None:
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").fill_null(fill_value)])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


@pytest.mark.parametrize(
    "float_dtype",
    [DataType.float32(), DataType.float64()],
)
def test_table_expr_fill_nan(float_dtype) -> None:
    input = [1.0, None, 3.0, float("nan")]
    fill_value = 2.0
    expected = [1.0, None, 3.0, 2.0]

    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(float_dtype).float.fill_nan(fill_value)])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected
