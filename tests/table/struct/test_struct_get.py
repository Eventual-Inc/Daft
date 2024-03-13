from __future__ import annotations

import datetime

import pytest

from daft.expressions import col
from daft.table import MicroPartition


def test_struct_get():
    table = MicroPartition.from_pydict(
        {
            "col": [
                {"foo": 1, "bar": "a"},
                {"foo": None, "bar": "b"},
                None,
                {"foo": 4, "bar": None},
            ]
        }
    )

    result = table.eval_expression_list([col("col").struct.get("foo"), col("col").struct.get("bar")])

    assert result.to_pydict() == {"foo": [1, None, None, 4], "bar": ["a", "b", None, None]}


def test_struct_get_logical_type():
    table = MicroPartition.from_pydict(
        {
            "col": [
                {"foo": datetime.date(2022, 1, 1)},
                {"foo": datetime.date(2022, 1, 2)},
                {"foo": None},
                None,
            ]
        }
    )

    result = table.eval_expression_list([col("col").struct.get("foo")])

    assert result.to_pydict() == {"foo": [datetime.date(2022, 1, 1), datetime.date(2022, 1, 2), None, None]}


def test_struct_get_bad_field():
    table = MicroPartition.from_pydict(
        {
            "col1": [
                {"foo": 1},
                {"foo": 2},
                {"foo": 3},
            ],
            "bar": ["a", "b", "c"],
        }
    )

    with pytest.raises(ValueError):
        table.eval_expression_list([col("col").struct.get("bar")])

    with pytest.raises(ValueError):
        table.eval_expression_list([col("bar").struct.get("foo")])
