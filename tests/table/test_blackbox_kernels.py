from __future__ import annotations

from daft.expressions import col
from daft.table import Table


def test_pyobjects_blackbox_kernels() -> None:
    objects = [object(), None, object()]
    table = Table.from_pydict({"keys": [0, 1, 2], "objs": objects})
    # Head.
    assert table.head(2).to_pydict()["objs"] == objects[:2]
    # Filter.
    assert table.filter([col("keys") > 0]).to_pydict()["objs"] == objects[1:]


def test_nested_blackbox_kernels() -> None:
    structs = [{"a": 1, "b": 2}, None, {"a": 3}]
    lists = [[1, 2], None, [3]]
    table = Table.from_pydict({"keys": [0, 1, 2], "structs": structs, "lists": lists})
    # pyarrow fills in implicit field-internal Nones on a .to_pylist() conversion.
    structs[2]["b"] = None
    # Head.
    head_result = table.head(2).to_pydict()
    assert head_result["structs"] == structs[:2]
    assert head_result["lists"] == lists[:2]
    # Filter.
    filter_result = table.filter([col("keys") > 0]).to_pydict()
    assert filter_result["structs"] == structs[1:]
    assert filter_result["lists"] == lists[1:]
