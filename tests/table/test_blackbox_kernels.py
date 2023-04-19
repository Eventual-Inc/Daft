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
