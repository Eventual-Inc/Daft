from __future__ import annotations

import daft


def test_map_rows_simple():
    df = daft.from_pydict({"x": [1, 2, 3]})

    output_schema = daft.Schema.from_pydict({"x": daft.DataType.int64(), "y": daft.DataType.string()})

    def f(row: dict[str, int]) -> dict[str, int | str]:
        original = row["x"]
        value = row["x"] * 2
        row["y"] = f"{original}*2 = {value}"
        return row

    actual = df.map_rows(f, output_schema).to_pydict()
    expected = {"x": [1, 2, 3], "y": ["1*2 = 2", "2*2 = 4", "3*2 = 6"]}

    assert actual == expected
