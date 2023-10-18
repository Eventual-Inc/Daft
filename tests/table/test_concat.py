from __future__ import annotations

import pytest

from daft.table import MicroPartition, Table


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_concat(TableCls) -> None:
    objs1 = [None, object(), object()]
    objs2 = [object(), None, object()]
    tables = [
        TableCls.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"], "z": objs1}),
        TableCls.from_pydict({"x": [4, 5, 6], "y": ["d", "e", "f"], "z": objs2}),
    ]

    result = TableCls.concat(tables)
    assert result.to_pydict() == {"x": [1, 2, 3, 4, 5, 6], "y": ["a", "b", "c", "d", "e", "f"], "z": objs1 + objs2}

    tables = [
        TableCls.from_pydict({"x": [], "y": []}),
        TableCls.from_pydict({"x": [], "y": []}),
    ]

    result = TableCls.concat(tables)
    assert result.to_pydict() == {"x": [], "y": []}


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_concat_bad_input(TableCls) -> None:
    mix_types_table = [TableCls.from_pydict({"x": [1, 2, 3]}), []]
    with pytest.raises(TypeError, match=f"Expected a {TableCls.__name__} for concat"):
        TableCls.concat(mix_types_table)

    with pytest.raises(ValueError, match=f"Need at least 1 {TableCls.__name__}"):
        TableCls.concat([])


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_concat_schema_mismatch(TableCls) -> None:
    mix_types_table = [
        TableCls.from_pydict({"x": [1, 2, 3]}),
        TableCls.from_pydict({"y": [1, 2, 3]}),
    ]

    with pytest.raises(ValueError, match=f"{TableCls.__name__} concat requires all schemas to match"):
        TableCls.concat(mix_types_table)

    mix_types_table = [
        TableCls.from_pydict({"x": [1, 2, 3]}),
        TableCls.from_pydict({"x": [1.0, 2.0, 3.0]}),
    ]

    with pytest.raises(ValueError, match=f"{TableCls.__name__} concat requires all schemas to match"):
        TableCls.concat(mix_types_table)

    mix_types_table = [
        TableCls.from_pydict({"x": [1, 2, 3]}),
        TableCls.from_pydict({"x": [object(), object(), object()]}),
    ]

    with pytest.raises(ValueError, match=f"{TableCls.__name__} concat requires all schemas to match"):
        TableCls.concat(mix_types_table)

    mix_types_table = [
        TableCls.from_pydict({"x": [1, 2, 3]}),
        TableCls.from_pydict({"x": [1, 2, 3], "y": [2, 3, 4]}),
    ]

    with pytest.raises(ValueError, match=f"{TableCls.__name__} concat requires all schemas to match"):
        TableCls.concat(mix_types_table)
