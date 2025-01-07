from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_binary_length():
    table = MicroPartition.from_pydict({"col": [b"Hello", b"\xff\xfe\x00", b"", b"World!"]})

    result = table.eval_expression_list([col("col").binary.length()])
    assert result.to_pydict() == {"col": [5, 3, 0, 6]}

    # Test with nulls
    table = MicroPartition.from_pydict({"col": [b"Hello", None, b"", b"World!", None]})

    result = table.eval_expression_list([col("col").binary.length()])
    assert result.to_pydict() == {"col": [5, None, 0, 6, None]}
