from __future__ import annotations

import re

import numpy as np
import pandas as pd
import pytest
from PIL import Image

import daft
from tests.utils import ANSI_ESCAPE

# Updated styles to match the new HTML structure
TD_STYLE = 'style="text-align:left; width: calc(100vw / 2); min-width: 192px; max-height: 100px; overflow: hidden; text-overflow: ellipsis; word-wrap: break-word; overflow-y: auto"'
TH_STYLE = 'style="text-wrap: nowrap; width: calc(100vw / 2); min-width: 192px; overflow: hidden; text-overflow: ellipsis; text-align:left"'

# Old styles for unmaterialized dataframes (schema-only display)
OLD_TD_STYLE = 'style="text-align:left; max-width:192px; max-height:64px; overflow:auto"'
OLD_TH_STYLE = 'style="text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left"'

ROW_DIVIDER_REGEX = re.compile(r"╭─+┬*─*╮|├╌+┼*╌+┤")
SHOWING_N_ROWS_REGEX = re.compile(r".*\(Showing first (\d+) of (\d+) rows\).*")
UNMATERIALIZED_REGEX = re.compile(r".*\(No data to display: Dataframe not materialized\).*")
MATERIALIZED_NO_ROWS_REGEX = re.compile(r".*\(No data to display: Materialized dataframe has no rows\).*")


def parse_str_table(
    table: str, expected_user_msg_regex: re.Pattern = SHOWING_N_ROWS_REGEX
) -> dict[str, tuple[str, list[str]]]:
    table = ANSI_ESCAPE.sub("", table)

    def _split_table_row(row: str) -> list[str]:
        return [cell.strip() for cell in re.split("┆|│", row)[1:-1]]

    lines = table.split("\n")
    assert len(lines) > 4
    assert ROW_DIVIDER_REGEX.match(lines[0])
    assert expected_user_msg_regex.match(lines[-1])

    column_names = _split_table_row(lines[1])
    column_types = _split_table_row(lines[3])

    data = []
    for line in lines[5:-3]:
        if ROW_DIVIDER_REGEX.match(line):
            continue
        data.append(_split_table_row(line))
    val = {column_names[i]: (column_types[i], [row[i] for row in data]) for i in range(len(column_names))}
    return val


def parse_html_table(
    table: str, expected_user_msg_regex: re.Pattern = SHOWING_N_ROWS_REGEX
) -> dict[str, tuple[str, list[str]]]:
    lines = table.split("\n")
    assert lines[0].strip() == "<div>"
    assert lines[-1].strip() == "</div>"
    assert expected_user_msg_regex.match(lines[-2].strip())

    html_table = lines[1:-2]

    # Pandas has inconsistent behavior when parsing <br> tags, so we manually replace it with a space
    html_table = [line.replace("<br>", " ") for line in html_table]

    pd_df = pd.read_html("\n".join(html_table))[0]

    # If only one HTML row, then the table is empty and Pandas has backward incompatible parsing behavior
    # between certain versions, so we parse the HTML ourselves.
    num_html_rows = sum([line.count("<tr>") for line in html_table])
    if num_html_rows == 1:
        result = {}
        for idx in range(len(pd_df.columns)):
            name, dtype = str(pd_df.iloc[0, idx]).split(" ")
            result[name] = (dtype, [])
        return result

    # More than one HTML row, so Pandas table correctly parses headers and body
    result = {}
    for table_key, table_values in pd_df.to_dict().items():
        name, dtype = table_key.split(" ")
        result[name] = (dtype, [str(table_values[idx]) for idx in range(len(table_values))])
    return result


def test_empty_repr(make_df):
    df = daft.from_pydict({})
    df = daft.from_pydict({})
    assert df.__repr__() == "(No data to display: Dataframe has no columns)"
    assert df._repr_html_() == "<small>(No data to display: Dataframe has no columns)</small>"

    df.collect()
    assert df.__repr__() == "(No data to display: Dataframe has no columns)"
    assert df._repr_html_() == "<small>(No data to display: Dataframe has no columns)</small>"


@pytest.mark.parametrize("num_preview_rows", [9, 10, None])
def test_repr_with_non_default_preview_rows(make_df, num_preview_rows):
    df = make_df({"A": [i for i in range(10)], "B": [i for i in range(10)]})
    df.collect(num_preview_rows=num_preview_rows)
    df.__repr__()

    assert df._preview.total_rows == 10
    assert len(df._preview.partition) == (num_preview_rows if num_preview_rows is not None else 10)


def test_empty_df_repr(make_df):
    df = make_df({"A": [1, 2, 3], "B": ["a", "b", "c"]})
    df = df.where(df["A"] > 10)
    expected_data = {"A": ("Int64", []), "B": ("Utf8", [])}

    assert parse_str_table(df.__repr__(), expected_user_msg_regex=UNMATERIALIZED_REGEX) == expected_data
    assert (
        df._repr_html_()
        == f"""<div>
<table class="dataframe">
<thead><tr><th {OLD_TH_STYLE}>A<br />Int64</th><th {OLD_TH_STYLE}>B<br />Utf8</th></tr></thead>
</table>
<small>(No data to display: Dataframe not materialized)</small>
</div>"""
    )

    df.collect()
    expected_data = {
        "A": (
            "Int64",
            [],
        ),
        "B": (
            "Utf8",
            [],
        ),
    }
    assert parse_str_table(df.__repr__(), expected_user_msg_regex=MATERIALIZED_NO_ROWS_REGEX) == expected_data
    assert (
        df._repr_html_()
        == f"""<div>
<table class="dataframe" style="table-layout: fixed; min-width: 100%">
<thead><tr><th {TH_STYLE}>A<br />Int64</th><th {TH_STYLE}>B<br />Utf8</th></tr></thead>
<tbody>
</tbody>
</table>
<small>(No data to display: Materialized dataframe has no rows)</small>
</div>"""
    )


def test_alias_repr(make_df):
    df = make_df({"A": [1, 2, 3], "B": ["a", "b", "c"]})
    df = df.select(df["A"].alias("A2"), df["B"])

    expected_data = {"A2": ("Int64", []), "B": ("Utf8", [])}
    assert parse_str_table(df.__repr__(), expected_user_msg_regex=UNMATERIALIZED_REGEX) == expected_data
    assert (
        df._repr_html_()
        == f"""<div>
<table class="dataframe">
<thead><tr><th {OLD_TH_STYLE}>A2<br />Int64</th><th {OLD_TH_STYLE}>B<br />Utf8</th></tr></thead>
</table>
<small>(No data to display: Dataframe not materialized)</small>
</div>"""
    )

    df.collect()

    expected_data = {
        "A2": (
            "Int64",
            ["1", "2", "3"],
        ),
        "B": (
            "Utf8",
            ["a", "b", "c"],
        ),
    }
    assert parse_str_table(df.__repr__()) == expected_data
    assert (
        df._repr_html_()
        == f"""<div>
<table class="dataframe" style="table-layout: fixed; min-width: 100%">
<thead><tr><th {TH_STYLE}>A2<br />Int64</th><th {TH_STYLE}>B<br />Utf8</th></tr></thead>
<tbody>
<tr><td data-row="0" data-col="0"><div {TD_STYLE}>1</div></td><td data-row="0" data-col="1"><div {TD_STYLE}>a</div></td></tr>
<tr><td data-row="1" data-col="0"><div {TD_STYLE}>2</div></td><td data-row="1" data-col="1"><div {TD_STYLE}>b</div></td></tr>
<tr><td data-row="2" data-col="0"><div {TD_STYLE}>3</div></td><td data-row="2" data-col="1"><div {TD_STYLE}>c</div></td></tr>
</tbody>
</table>
<small>(Showing first 3 of 3 rows)</small>
</div>"""
    )


def test_repr_with_unicode(make_df, data_source):
    df = make_df({"🔥": [1, 2, 3], "🦁": ["🔥a", "b🔥", "🦁🔥" * 60]})
    expected_data_unmaterialized = {"🔥": ("Int64", []), "🦁": ("Utf8", [])}
    expected_data_materialized = {
        "🔥": (
            "Int64",
            ["1", "2", "3"],
        ),
        "🦁": (
            "Utf8",
            ["🔥a", "b🔥", "🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁🔥🦁…"],
        ),
    }

    string_array = ["🔥a", "b🔥", "🦁🔥" * 60]  # we dont truncate for html
    # For 2 columns, use calc(100vw / 2)
    th_style_2col = 'style="text-wrap: nowrap; width: calc(100vw / 2); min-width: 192px; overflow: hidden; text-overflow: ellipsis; text-align:left"'
    td_style_2col = 'style="text-align:left; width: calc(100vw / 2); min-width: 192px; max-height: 100px; overflow: hidden; text-overflow: ellipsis; word-wrap: break-word; overflow-y: auto"'
    expected_html_unmaterialized = f"""<div>
<table class="dataframe">
<thead><tr><th {OLD_TH_STYLE}>🔥<br />Int64</th><th {OLD_TH_STYLE}>🦁<br />Utf8</th></tr></thead>
</table>
<small>(No data to display: Dataframe not materialized)</small>
</div>"""
    expected_html_materialized = f"""<div>
<table class="dataframe" style="table-layout: fixed; min-width: 100%">
<thead><tr><th {th_style_2col}>🔥<br />Int64</th><th {th_style_2col}>🦁<br />Utf8</th></tr></thead>
<tbody>
<tr><td data-row="0" data-col="0"><div {td_style_2col}>1</div></td><td data-row="0" data-col="1"><div {td_style_2col}>{string_array[0]}</div></td></tr>
<tr><td data-row="1" data-col="0"><div {td_style_2col}>2</div></td><td data-row="1" data-col="1"><div {td_style_2col}>{string_array[1]}</div></td></tr>
<tr><td data-row="2" data-col="0"><div {td_style_2col}>3</div></td><td data-row="2" data-col="1"><div {td_style_2col}>{string_array[2]}</div></td></tr>
</tbody>
</table>
<small>(Showing first 3 of 3 rows)</small>
</div>"""

    variant = data_source
    if variant == "parquet":
        assert (
            parse_str_table(df.__repr__(), expected_user_msg_regex=UNMATERIALIZED_REGEX) == expected_data_unmaterialized
        )
        assert df._repr_html_() == expected_html_unmaterialized
    elif variant == "arrow":
        assert (
            parse_str_table(df.__repr__(), expected_user_msg_regex=SHOWING_N_ROWS_REGEX) == expected_data_materialized
        )
        assert df._repr_html_() == expected_html_materialized

    df.collect()

    assert parse_str_table(df.__repr__()) == expected_data_materialized
    assert df._repr_html_() == expected_html_materialized


def test_repr_with_html_string():
    df = daft.from_pydict({"A": [f"<div>body{i}</div>" for i in range(3)]})

    non_html_table = df.__repr__()
    html_table = df._repr_html_()
    for i in range(3):
        assert f"<div>body{i}</div>" in non_html_table
        # For 1 column, use calc(100vw / 1)
        td_style_1col = 'style="text-align:left; width: calc(100vw / 1); min-width: 192px; max-height: 100px; overflow: hidden; text-overflow: ellipsis; word-wrap: break-word; overflow-y: auto"'
        assert (
            f'<tr><td data-row="{i}" data-col="0"><div {td_style_1col}>&lt;div&gt;body{i}&lt;/div&gt;</div></td></tr>'
            in html_table
        )


class MyObj:
    def __repr__(self) -> str:
        return "myobj-custom-repr"


def test_repr_html_custom_hooks():
    img = Image.fromarray(np.ones((3, 3)).astype(np.uint8))
    arr = np.ones((3, 3))

    df = daft.from_pydict(
        {
            "objects": daft.Series.from_pylist([MyObj() for _ in range(3)], pyobj="force"),
            "np": daft.Series.from_pylist([arr for _ in range(3)], pyobj="force"),
            "pil": daft.Series.from_pylist([img for _ in range(3)], pyobj="force"),
        }
    )

    assert (
        ANSI_ESCAPE.sub("", df.__repr__()).replace("\r", "")
        == """╭───────────────────┬─────────────┬────────────────────────────────╮
│ objects           ┆ np          ┆ pil                            │
│ ---               ┆ ---         ┆ ---                            │
│ Python            ┆ Python      ┆ Python                         │
╞═══════════════════╪═════════════╪════════════════════════════════╡
│ myobj-custom-repr ┆ [[1. 1. 1.] ┆ <PIL.Image.Image image mode=L… │
│                   ┆  [1. 1. 1.] ┆                                │
│                   ┆  [1. …      ┆                                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ myobj-custom-repr ┆ [[1. 1. 1.] ┆ <PIL.Image.Image image mode=L… │
│                   ┆  [1. 1. 1.] ┆                                │
│                   ┆  [1. …      ┆                                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ myobj-custom-repr ┆ [[1. 1. 1.] ┆ <PIL.Image.Image image mode=L… │
│                   ┆  [1. 1. 1.] ┆                                │
│                   ┆  [1. …      ┆                                │
╰───────────────────┴─────────────┴────────────────────────────────╯

(Showing first 3 of 3 rows)"""
    )

    html_repr = df._repr_html_()

    # Assert that MyObj is correctly displayed in html repr (falls back to __repr__)
    assert "myobj-custom-repr" in html_repr

    # Assert that PIL viz hook correctly triggers in html repr
    assert '<img style="max-height:128px;width:auto" src="data:image/png;base64,' in html_repr

    # Assert that numpy array viz hook correctly triggers in html repr
    # For 3 columns, use calc(100vw / 3)
    td_style_3col = 'style="text-align:left; width: calc(100vw / 3); min-width: 192px; max-height: 100px; overflow: hidden; text-overflow: ellipsis; word-wrap: break-word; overflow-y: auto"'
    assert (
        f'<td data-row="0" data-col="1"><div {td_style_3col}>&ltnp.ndarray<br>shape=(3, 3)<br>dtype=float64&gt</div></td><td data-row="0" data-col="2">'
        in html_repr
    )


def test_repr_empty_struct():
    data = {"empty_structs": [{}, {}], "nested_empty_structs": [{"a": {}}, {"b": {}}]}
    df = daft.from_pydict(data)

    expected_schema_truncated_repr = """╭───────────────┬──────────────────────────────────╮
│ empty_structs ┆ nested_empty_structs             │
│ ---           ┆ ---                              │
│ Struct[]      ┆ Struct[a: Struct[], b: Struct[]] │
╰───────────────┴──────────────────────────────────╯
"""
    assert ANSI_ESCAPE.sub("", df.schema()._truncated_table_string()) == expected_schema_truncated_repr

    expected_schema_repr = """╭──────────────────────┬──────────────────────────────────╮
│ column_name          ┆ type                             │
╞══════════════════════╪══════════════════════════════════╡
│ empty_structs        ┆ Struct[]                         │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ nested_empty_structs ┆ Struct[a: Struct[], b: Struct[]] │
╰──────────────────────┴──────────────────────────────────╯
"""
    assert ANSI_ESCAPE.sub("", repr(df.schema())) == expected_schema_repr

    expected_repr = """╭───────────────┬──────────────────────────────────╮
│ empty_structs ┆ nested_empty_structs             │
│ ---           ┆ ---                              │
│ Struct[]      ┆ Struct[a: Struct[], b: Struct[]] │
╞═══════════════╪══════════════════════════════════╡
│ {}            ┆ {a: {},                          │
│               ┆ b: None,                         │
│               ┆ }                                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ {}            ┆ {a: None,                        │
│               ┆ b: {},                           │
│               ┆ }                                │
╰───────────────┴──────────────────────────────────╯

(Showing first 2 of 2 rows)"""

    assert ANSI_ESCAPE.sub("", str(df)) == expected_repr


def test_interactive_html_with_record_batch():
    """Test interactive HTML generation with a RecordBatch directly."""
    from daft.dataframe.preview import PreviewFormatter

    # Create a DataFrame and get its RecordBatch
    df = daft.from_pydict({"A": [1, 2, 3], "B": ["a", "b", "c"]})
    df.collect()

    # Create a PreviewFormatter and generate interactive HTML
    preview = df._preview
    schema = df.schema()
    formatter = PreviewFormatter(preview, schema)
    html = formatter._generate_interactive_html()

    # Test that the HTML contains the expected structure
    assert "<style>" in html
    assert '<table class="dataframe" style="table-layout: fixed; min-width: 100%">' in html
    assert "<thead>" in html
    assert "<tbody>" in html
    assert "</table>" in html

    # Test that it contains the side pane structure
    assert "side-pane" in html
    assert "side-pane-header" in html
    assert "side-pane-title" in html

    # Test that it contains JavaScript for interactivity
    assert "showSidePane" in html
    assert "onclick" in html
    assert "fetch(" in html

    # Test that it contains CSS styles
    assert "cursor: pointer" in html
    assert "display: flex" in html

    # Test that the data attributes are present for cell identification
    assert "data-row=" in html
    assert "data-col=" in html

    # Test that the server URL is embedded in the JavaScript
    assert "127.0.0.1:3238" in html

    # Test that the table contains the expected data
    assert "A" in html
    assert "B" in html
    assert "Int64" in html
    assert "Utf8" in html
    assert "1" in html
    assert "2" in html
    assert "3" in html
    assert "a" in html
    assert "b" in html
    assert "c" in html
