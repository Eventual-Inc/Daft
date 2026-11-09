from __future__ import annotations

from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_substr():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barbarbar", "quux", "1", ""]})
    result = table.eval_expression_list([col("col").substr(0, 5)])
    assert result.to_pydict() == {"col": ["foo", None, "barba", "quux", "1", ""]}


def test_utf8_substr_empty_results_are_empty_strings():
    table = MicroPartition.from_pydict(
        {
            "col": ["hello", "hello", "", "☃😉🌈", "☃😉🌈"],
            "start": [0, 5, 0, 3, 4],
            "length": [0, 2, 3, 1, 1],
        }
    )

    result = table.eval_expression_list([col("col").substr(col("start"), col("length"))])

    assert result.to_pydict() == {"col": ["", "", "", "", ""]}
