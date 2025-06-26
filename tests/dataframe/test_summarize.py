from __future__ import annotations

import daft


def test_summarize_sanity():
    df = daft.from_pydict(
        {
            "A": [1, 2, 3, 4, 5],
            "B": [1.5, 2.5, 3.5, 4.5, 5.5],
            "C": [True, True, False, False, None],
            "D": [None, None, None, None, None],
        }
    )
    # row for each column of input
    assert df.summarize().count_rows() == 4
    assert df.select("A", "B").summarize().count_rows() == 2


def test_summarize_dataframe(make_df, valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pydict(
        {
            "a": [1, 2, 3, 3],
            "b": [None, "a", "b", "c"],
        }
    )
    expected = {
        "column": ["a", "b"],
        "type": ["Int64", "Utf8"],
        "min": ["1", "a"],
        "max": ["3", "c"],
        "count": [4, 3],
        "count_nulls": [0, 1],
        "approx_count_distinct": [3, 3],
    }
    assert df.summarize().to_pydict() == expected
