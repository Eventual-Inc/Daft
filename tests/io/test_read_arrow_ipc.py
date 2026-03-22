from __future__ import annotations

from pathlib import Path

import pyarrow as pa

import daft


def test_read_arrow_ipc_basic(tmp_path: Path) -> None:
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    path = tmp_path / "test.arrow"
    with pa.OSFile(str(path), "wb") as sink:
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)

    df = daft.read_arrow_ipc(str(path))
    assert df.column_names == ["a", "b"]
    assert df.count_rows() == 3

    out = df.to_pydict()
    assert out["a"] == [1, 2, 3]
    assert out["b"] == ["x", "y", "z"]
