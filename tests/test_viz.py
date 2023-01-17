from __future__ import annotations

import re

from daft import DataFrame
from daft.viz import DataFrameDisplay

ROW_DIVIDER_REGEX = re.compile(r"\+-+\+")
SHOWING_N_ROWS_REGEX = re.compile(r"\(Showing first (\d+) of (\d+) rows\)")


def parse_str_table(table) -> dict[str, tuple[str, list[str]]]:
    def _split_table_row(row: str) -> list[str]:
        return [cell.strip() for cell in row.split("|")[1:-1]]

    lines = table.split("\n")
    assert len(lines) > 4
    assert ROW_DIVIDER_REGEX.match(lines[0])
    assert SHOWING_N_ROWS_REGEX.match(lines[-1])

    column_names = _split_table_row(lines[1])
    column_types = _split_table_row(lines[2])

    data = []
    for line in lines[4:-1]:
        if ROW_DIVIDER_REGEX.match(line):
            continue
        data.append(_split_table_row(line))

    return {column_names[i]: (column_types[i], [row[i] for row in data]) for i in range(len(column_names))}


def test_alias_repr():
    df = DataFrame.from_pydict({"A": [1, 2, 3]})
    df = df.select(df["A"].alias("A2"))
    df.collect()
    display = DataFrameDisplay(df._preview, df.schema())
    repr_str = display.__repr__()

    assert parse_str_table(repr_str) == {
        "A2": (
            "INTEGER",
            ["1", "2", "3"],
        )
    }
