from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_base64_decode():
    table = MicroPartition.from_pydict({"col": ["Zm9v", "QmFy", "QlVaei4="]})
    result = table.eval_expression_list([col("col").str.base64_decode()])
    assert result.to_pydict() == {"col": ["foo", "Bar", "BUZz."]}
