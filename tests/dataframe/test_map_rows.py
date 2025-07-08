from __future__ import annotations

import daft


def test_map_rows_simple():
    df = daft.from_pydict({"x": [1, 2, 3]})

    output_schema = daft.Schema.from_pydict({"x": daft.DataType.int64(), "y": daft.DataType.int64()})

    def f(row):
        row["y"] = row["x"] * 2
        return row

    actual = df.map_rows(f, output_schema).to_pydict()

    expected = {"x": [1, 2, 3], "y": [2, 4, 6]}

    assert actual == expected
