# fmt: off
# ^ ruff formatting breaks tests which are sensitive to format.

from __future__ import annotations

import daft
from daft.dataframe import DataFrame
from daft.dataframe.preview import PreviewFormat, PreviewFormatter


def test_show_default(make_df, valid_data):
    df: DataFrame = make_df(valid_data)
    df_preview = df._construct_show_preview(8)

    assert len(df_preview.partition) == len(valid_data)

    # Limit was greater than the actual total rows, so cap at the valid length
    assert df_preview.total_rows == 3


def test_show_some(make_df, valid_data, data_source):
    df = make_df(valid_data)
    df_preview = df._construct_show_preview(1)

    # Check the partition was trimmed to the preview length.
    assert len(df_preview.partition) == 1

    # Limit is less than DataFrame length, so we only know full DataFrame length if it was loaded from memory, e.g. arrow.
    variant = data_source
    if variant == "parquet":
        assert df_preview.total_rows is None
    elif variant == "arrow":
        assert df_preview.total_rows == len(valid_data)


def test_show_from_cached_repr(make_df, valid_data):
    df = make_df(valid_data)
    df = df.collect()
    df.__repr__()
    collected_preview = df._preview
    df_preview = df._construct_show_preview(8)

    # Check that cached preview from df.__repr__() was used.
    assert df_preview is collected_preview

    # Check lengths are valid
    assert len(df_preview.partition) == len(valid_data)
    assert df_preview.total_rows == 3


def test_show_from_cached_repr_prefix(make_df, valid_data):
    df = make_df(valid_data)
    df = df.collect(3)
    df.__repr__()
    df_preview = df._construct_show_preview(2)

    # Check that a prefix of the cached preview from df.__repr__() was used, so dataframe_total_rows should be set.
    assert len(df_preview.partition) == 2
    assert df_preview.total_rows == 3


def test_show_not_from_cached_repr(make_df, valid_data, data_source):
    df = make_df(valid_data)
    df = df.collect(2)
    df.__repr__()
    collected_preview = df._preview
    
    variant = data_source
    if variant == "parquet":
        # For parquet, when requesting more rows than collected, we need to read from file.
        # Cached preview is NOT USED because data was not fully materialized from parquet.
        df_preview = df._construct_show_preview(8)
        assert df_preview != collected_preview
        # Check lengths are valid - we should get all 3 rows
        assert len(df_preview.partition) == len(valid_data)
        assert df_preview.total_rows == 3
    elif variant == "arrow":
        # For arrow, data is fully materialized in memory.
        # When requesting the same number of rows as collected, cached preview should be reused.
        df_preview_same = df._construct_show_preview(2)
        assert df_preview_same is collected_preview
        
        # When requesting more rows than collected, we need to fetch more from materialized data.
        # Cached preview is NOT USED because it doesn't have enough rows.
        df_preview_more = df._construct_show_preview(8)
        assert df_preview_more != collected_preview
        # Check lengths are valid - we should get all 3 rows
        assert len(df_preview_more.partition) == len(valid_data)
        assert df_preview_more.total_rows == 3


###
# tests for .show() with formatting options
###


def show(df: DataFrame, format: PreviewFormat, **options):
    """Helper since .show() will print to stdout and have user messages."""
    return PreviewFormatter(df._preview, df.schema(), format, **options)._to_text()


def test_show_with_options():
    df = daft.from_pydict(
        {
            "A": [1, 2, 3, 4],
            "B": [1.5, 2.5, 3.5, 4.5],
            "C": [True, True, False, False],
            "D": [None, None, None, None],
        }
    )

    columns = [
        {
            "info": "units",
            "align": "right",
        },
        {
            "info": "kg",
            "align": "right",
        },
        {},
        {},
    ]

    assert (
        show(df, format="markdown")
        == """
| A | B   | C     | D    |
|---|-----|-------|------|
| 1 | 1.5 | true  | None |
| 2 | 2.5 | true  | None |
| 3 | 3.5 | false | None |
| 4 | 4.5 | false | None |"""[1:]
    )

    assert (
        show(df, format="markdown", verbose=True)
        == """
| A (Int64) | B (Float64) | C (Bool) | D (Null) |
|-----------|-------------|----------|----------|
| 1         | 1.5         | true     | None     |
| 2         | 2.5         | true     | None     |
| 3         | 3.5         | false    | None     |
| 4         | 4.5         | false    | None     |"""[1:]
    )

    assert (
        show(df, format="markdown", verbose=True, columns=columns)
        == """
| A (units) | B (kg) | C (Bool) | D (Null) |
|-----------|--------|----------|----------|
|         1 |    1.5 | true     | None     |
|         2 |    2.5 | true     | None     |
|         3 |    3.5 | false    | None     |
|         4 |    4.5 | false    | None     |"""[1:]
    )


def test_show_with_wide_columns():
    df = daft.from_pydict(
        {
            "A": ["This is a very long text that exceeds the default max_width."],
            "B": ["Another extremely long piece of text that also exceeds the default max_width."],
        }
    )

    # default max_width
    assert show(df, format="markdown") == """
| A                            | B                            |
|------------------------------|------------------------------|
| This is a very long text th… | Another extremely long piec… |"""[1:]

    # explicit max_width
    assert show(df, format="markdown", max_width=12) == """
| A          | B          |
|------------|------------|
| This is a… | Another e… |"""[1:]

    # no max_width
    assert show(df, format="simple", max_width=None) == """
 A                                                              B                                                                             
--------------------------------------------------------------+-------------------------------------------------------------------------------
 This is a very long text that exceeds the default max_width.   Another extremely long piece of text that also exceeds the default max_width. """[1:]


def test_show_default_respects_options():
    df = daft.from_pydict(
        {
            "strings": ["this is a really long string that should not be be truncated"],
        }
    )

    out = show(df, format=None, max_width=1000)
    assert out == """
╭──────────────────────────────────────────────────────────────╮
│ strings                                                      │
╞══════════════════════════════════════════════════════════════╡
│ this is a really long string that should not be be truncated │
╰──────────────────────────────────────────────────────────────╯"""[1:]


def test_show_with_many_columns():
    df = daft.from_pylist(
        [
            {
                "A": 1,
                "B": 2,
                "C": 3,
                "D": 4,
                "E": 5,
                "F": 6,
                "G": 7,
                "H": 8,
                "I": 9,
                "J": 10,
                "K": 11,
                "L": 12,
                "M": 13,
                "N": 14,
                "O": 15,
                "P": 16,
                "Q": 17,
                "R": 18,
                "S": 19,
                "T": 20,
                "U": 21,
                "V": 22,
                "W": 23,
                "X": 24,
                "Y": 25,
                "Z": 26,
            }
        ]
    )
    assert show(df, format="simple") == """
 A   B   C   D   E   F   G   H   I   J    K    L    M    N    O    P    Q    R    S    T    U    V    W    X    Y    Z  
---+---+---+---+---+---+---+---+---+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----
 1   2   3   4   5   6   7   8   9   10   11   12   13   14   15   16   17   18   19   20   21   22   23   24   25   26 """[1:]


def test_show_empty_dataframe_no_error_thrown():
    # Construct an empty DataFrame and ensure preview construction does not error
    df = daft.from_pydict({"A": []})
    preview = df._construct_show_preview(8)
    assert len(preview.partition) == 0
    assert preview.total_rows == 0
