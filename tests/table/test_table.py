from __future__ import annotations

import pyarrow as pa

from daft.daft import PyTable


def test_from_arrow_round_trip() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = PyTable.from_arrow_record_batches(pa_table.to_batches())
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]
    read_back = pa.Table.from_batches([daft_table.to_arrow_record_batch()])
    assert pa_table == read_back


def test_table_head() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = PyTable.from_arrow_record_batches(pa_table.to_batches())
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    # subslice
    headed = daft_table.head(3)
    assert len(headed) == 3
    assert headed.column_names() == ["a", "b"]
    pa_headed = pa.Table.from_batches([headed.to_arrow_record_batch()])
    assert pa_table[:3] == pa_headed

    # overslice
    headed = daft_table.head(5)
    assert len(headed) == 4
    assert headed.column_names() == ["a", "b"]
    pa_headed = pa.Table.from_batches([headed.to_arrow_record_batch()])
    assert pa_table == pa_headed
