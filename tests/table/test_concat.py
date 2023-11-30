from __future__ import annotations

import pytest

from daft.table import MicroPartition


def test_table_concat() -> None:
    objs1 = [None, object(), object()]
    objs2 = [object(), None, object()]
    tables = [
        MicroPartition.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"], "z": objs1}),
        MicroPartition.from_pydict({"x": [4, 5, 6], "y": ["d", "e", "f"], "z": objs2}),
    ]

    result = MicroPartition.concat(tables)
    assert result.to_pydict() == {"x": [1, 2, 3, 4, 5, 6], "y": ["a", "b", "c", "d", "e", "f"], "z": objs1 + objs2}

    tables = [
        MicroPartition.from_pydict({"x": [], "y": []}),
        MicroPartition.from_pydict({"x": [], "y": []}),
    ]

    result = MicroPartition.concat(tables)
    assert result.to_pydict() == {"x": [], "y": []}


def test_table_concat_bad_input() -> None:
    mix_types_table = [MicroPartition.from_pydict({"x": [1, 2, 3]}), []]
    with pytest.raises(TypeError, match=f"Expected a {MicroPartition.__name__} for concat"):
        MicroPartition.concat(mix_types_table)

    with pytest.raises(ValueError, match=f"Need at least 1 {MicroPartition.__name__}"):
        MicroPartition.concat([])


def test_table_concat_schema_mismatch() -> None:
    mix_types_table = [
        MicroPartition.from_pydict({"x": [1, 2, 3]}),
        MicroPartition.from_pydict({"y": [1, 2, 3]}),
    ]

    with pytest.raises(ValueError, match=f"{MicroPartition.__name__} concat requires all schemas to match"):
        MicroPartition.concat(mix_types_table)

    mix_types_table = [
        MicroPartition.from_pydict({"x": [1, 2, 3]}),
        MicroPartition.from_pydict({"x": [1.0, 2.0, 3.0]}),
    ]

    with pytest.raises(ValueError, match=f"{MicroPartition.__name__} concat requires all schemas to match"):
        MicroPartition.concat(mix_types_table)

    mix_types_table = [
        MicroPartition.from_pydict({"x": [1, 2, 3]}),
        MicroPartition.from_pydict({"x": [object(), object(), object()]}),
    ]

    with pytest.raises(ValueError, match=f"{MicroPartition.__name__} concat requires all schemas to match"):
        MicroPartition.concat(mix_types_table)

    mix_types_table = [
        MicroPartition.from_pydict({"x": [1, 2, 3]}),
        MicroPartition.from_pydict({"x": [1, 2, 3], "y": [2, 3, 4]}),
    ]

    with pytest.raises(ValueError, match=f"{MicroPartition.__name__} concat requires all schemas to match"):
        MicroPartition.concat(mix_types_table)
