from __future__ import annotations

from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_substr():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barbarbar", "quux", "1", ""]})
    result = table.eval_expression_list([col("col").substr(0, 5)])
    assert result.to_pydict() == {"col": ["foo", None, "barba", "quux", "1", ""]}


def test_utf8_substr_zero_length():
    table = MicroPartition.from_pydict({"col": ["foo", "你好"]})
    result = table.eval_expression_list([col("col").substr(0, 0)])
    assert result.to_pydict() == {"col": ["", ""]}


def test_utf8_substr_start_past_end():
    table = MicroPartition.from_pydict({"col": ["foo", "你好"]})
    result = table.eval_expression_list([col("col").substr(10, 2)])
    assert result.to_pydict() == {"col": ["", ""]}
