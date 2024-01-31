from __future__ import annotations

from daft.datatype import DataType
from daft.table.micropartition import MicroPartition
from daft.table.table import Table


def test_monotonically_increasing_id() -> None:
    table = MicroPartition.from_pydict({"a": [1, 2, 3, 4, 5]})
    table = table.add_monotonically_increasing_id(0, "id")

    assert len(table) == 5
    assert set(table.column_names()) == {"id", "a"}
    assert set(table.schema().column_names()) == {"id", "a"}
    assert table.schema()["id"].dtype == DataType.uint64()
    assert table.to_pydict() == {"id": [0, 1, 2, 3, 4], "a": [1, 2, 3, 4, 5]}


def test_monotonically_increasing_id_empty_table() -> None:
    table = MicroPartition.from_pydict({"a": []})
    table = table.add_monotonically_increasing_id(0, "id")

    assert len(table) == 0
    assert set(table.column_names()) == {"id", "a"}
    assert set(table.schema().column_names()) == {"id", "a"}
    assert table.schema()["id"].dtype == DataType.uint64()
    assert table.to_pydict() == {"id": [], "a": []}


def test_monotonically_increasing_id_multiple_tables_in_micropartition() -> None:
    table1 = Table.from_pydict({"a": [1, 2, 3]})
    table2 = Table.from_pydict({"a": [4, 5, 6]})
    table3 = Table.from_pydict({"a": [7, 8, 9]})

    table = MicroPartition._from_tables([table1, table2, table3])
    table = table.add_monotonically_increasing_id(0, "id")

    assert len(table) == 9
    assert set(table.column_names()) == {"id", "a"}
    assert set(table.schema().column_names()) == {"id", "a"}
    assert table.schema()["id"].dtype == DataType.uint64()
    assert table.to_pydict() == {"id": [0, 1, 2, 3, 4, 5, 6, 7, 8], "a": [1, 2, 3, 4, 5, 6, 7, 8, 9]}
