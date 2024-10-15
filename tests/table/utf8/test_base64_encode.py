from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_base64_encode():
    table = MicroPartition.from_pydict({"col": ["foo", "Bar", "BUZZ"]})
    result = table.eval_expression_list([col("col").str.base64_encode()])
    assert result.to_pydict() == {"col": ["Zm9v", "QmFy", "QlVaWg=="]}
