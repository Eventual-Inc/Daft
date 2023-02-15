from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions2 import lit
from daft.table import Table


@pytest.mark.parametrize(
    "data, expected_dtype",
    [
        (1, DataType.int32()),
        (2**32, DataType.int64()),
        (1 << 63, DataType.uint64()),
        (1.2, DataType.float64()),
        ("a", DataType.string()),
        (b"a", DataType.binary()),
        (True, DataType.bool()),
        (None, DataType.null()),
    ],
)
def test_make_lit(data, expected_dtype) -> None:
    l = lit(data)
    assert l.name() == "literal"
    empty_table = Table.empty()
    lit_table = empty_table.eval_expression_list([l])
    series = lit_table.get_column("literal")
    assert series.datatype() == expected_dtype
