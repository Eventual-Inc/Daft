from __future__ import annotations

import re

import numpy as np
import pandas as pd
from PIL import Image

import daft

ROW_DIVIDER_REGEX = re.compile(r"\+-+\+")
SHOWING_N_ROWS_REGEX = re.compile(r".*\(Showing first (\d+) of (\d+) rows\).*")
UNMATERIALIZED_REGEX = re.compile(r".*\(No data to display: Dataframe not materialized\).*")
MATERIALIZED_NO_ROWS_REGEX = re.compile(r".*\(No data to display: Materialized dataframe has no rows\).*")
TD_STYLE = 'style="text-align:left; max-width:192px; max-height:64px; overflow:auto"'


def parse_str_table(
    table: str, expected_user_msg_regex: re.Pattern = SHOWING_N_ROWS_REGEX
) -> dict[str, tuple[str, list[str]]]:
    def _split_table_row(row: str) -> list[str]:
        return [cell.strip() for cell in row.split("|")[1:-1]]

    lines = table.split("\n")
    assert len(lines) > 4
    assert ROW_DIVIDER_REGEX.match(lines[0])
    assert expected_user_msg_regex.match(lines[-1])

    column_names = _split_table_row(lines[1])
    column_types = _split_table_row(lines[2])

    data = []
    for line in lines[4:-2]:
        if ROW_DIVIDER_REGEX.match(line):
            continue
        data.append(_split_table_row(line))

    return {column_names[i]: (column_types[i], [row[i] for row in data]) for i in range(len(column_names))}


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


def test_empty_repr():
    df = daft.from_pydict({})
    assert df.__repr__() == "(No data to display: Dataframe has no columns)"
    assert df._repr_html_() == "<small>(No data to display: Dataframe has no columns)</small>"

    df.collect()
    assert df.__repr__() == "(No data to display: Dataframe has no columns)"
    assert df._repr_html_() == "<small>(No data to display: Dataframe has no columns)</small>"


def test_empty_df_repr():
    df = daft.from_pydict({"A": [1, 2, 3], "B": ["a", "b", "c"]})
    df = df.where(df["A"] > 10)
    expected_data = {"A": ("Int64", []), "B": ("Utf8", [])}

    assert parse_str_table(df.__repr__(), expected_user_msg_regex=UNMATERIALIZED_REGEX) == expected_data
    assert (
        df._repr_html_()
        == """<div>
<table class="dataframe">
<thead><tr><th style="text-wrap: nowrap; max-width:192px; overflow:auto">A<br />Int64</th><th style="text-wrap: nowrap; max-width:192px; overflow:auto">B<br />Utf8</th></tr></thead>
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
        == """<div>
<table class="dataframe">
<thead><tr><th style="text-wrap: nowrap; max-width:192px; overflow:auto">A<br />Int64</th><th style="text-wrap: nowrap; max-width:192px; overflow:auto">B<br />Utf8</th></tr></thead>
<tbody>
</tbody>
</table>
<small>(No data to display: Materialized dataframe has no rows)</small>
</div>"""
    )


def test_alias_repr():
    df = daft.from_pydict({"A": [1, 2, 3], "B": ["a", "b", "c"]})
    df = df.select(df["A"].alias("A2"), df["B"])

    expected_data = {"A2": ("Int64", []), "B": ("Utf8", [])}
    assert parse_str_table(df.__repr__(), expected_user_msg_regex=UNMATERIALIZED_REGEX) == expected_data
    assert (
        df._repr_html_()
        == """<div>
<table class="dataframe">
<thead><tr><th style="text-wrap: nowrap; max-width:192px; overflow:auto">A2<br />Int64</th><th style="text-wrap: nowrap; max-width:192px; overflow:auto">B<br />Utf8</th></tr></thead>
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
    expected_data_html = {
        **expected_data,
    }
    assert parse_str_table(df.__repr__()) == expected_data
    assert (
        df._repr_html_()
        == f"""<div>
<table class="dataframe">
<thead><tr><th style="text-wrap: nowrap; max-width:192px; overflow:auto">A2<br />Int64</th><th style="text-wrap: nowrap; max-width:192px; overflow:auto">B<br />Utf8</th></tr></thead>
<tbody>
<tr><td><div {TD_STYLE}>1</div></td><td><div {TD_STYLE}>a</div></td></tr>
<tr><td><div {TD_STYLE}>2</div></td><td><div {TD_STYLE}>b</div></td></tr>
<tr><td><div {TD_STYLE}>3</div></td><td><div {TD_STYLE}>c</div></td></tr>
</tbody>
</table>
<small>(Showing first 3 of 3 rows)</small>
</div>"""
    )


def test_repr_with_html_string():
    df = daft.from_pydict({"A": [f"<div>body{i}</div>" for i in range(3)]})
    df.collect()

    non_html_table = df.__repr__()
    html_table = df._repr_html_()
    for i in range(3):
        assert f"<div>body{i}</div>" in non_html_table
        assert f"<tr><td><div {TD_STYLE}>&lt;div&gt;body{i}&lt;/div&gt;</div></td></tr>" in html_table


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
    df.collect()

    assert (
        df.__repr__().replace("\r", "")
        == """+-------------------+-------------+----------------------------------+
| objects           | np          | pil                              |
| Python            | Python      | Python                           |
+-------------------+-------------+----------------------------------+
| myobj-custom-repr | [[1. 1. 1.] | <PIL.Image.Image image mode=L... |
|                   |  [1. 1. 1.] |                                  |
|                   |  [1. ...    |                                  |
+-------------------+-------------+----------------------------------+
| myobj-custom-repr | [[1. 1. 1.] | <PIL.Image.Image image mode=L... |
|                   |  [1. 1. 1.] |                                  |
|                   |  [1. ...    |                                  |
+-------------------+-------------+----------------------------------+
| myobj-custom-repr | [[1. 1. 1.] | <PIL.Image.Image image mode=L... |
|                   |  [1. 1. 1.] |                                  |
|                   |  [1. ...    |                                  |
+-------------------+-------------+----------------------------------+

(Showing first 3 of 3 rows)"""
    )

    html_repr = df._repr_html_()

    # Assert that MyObj is correctly displayed in html repr (falls back to __repr__)
    assert "myobj-custom-repr" in html_repr

    # Assert that PIL viz hook correctly triggers in html repr
    assert 'alt="<PIL.Image.Image image mode=L size=3x3' in html_repr
    assert '<img style="max-height:128px;width:auto" src="data:image/png;base64,' in html_repr

    # Assert that numpy array viz hook correctly triggers in html repr
    assert f"<td><div {TD_STYLE}>&ltnp.ndarray<br>shape=(3, 3)<br>dtype=float64&gt</div></td><td>" in html_repr
