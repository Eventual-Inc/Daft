from __future__ import annotations

import pyarrow as pa
import pytest

from daft.table import Table


def test_table_head() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    # subslice
    headed = daft_table.head(3)
    assert len(headed) == 3
    assert headed.column_names() == ["a", "b"]
    pa_headed = headed.to_arrow()
    assert pa_table[:3] == pa_headed

    # overslice
    headed = daft_table.head(5)
    assert len(headed) == 4
    assert headed.column_names() == ["a", "b"]
    pa_headed = headed.to_arrow()
    assert pa_table == pa_headed

    # negative slice
    with pytest.raises(ValueError, match="negative number"):
        headed = daft_table.head(-1)
