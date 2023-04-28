from __future__ import annotations

import re

import pandas as pd

import daft

ROW_DIVIDER_REGEX = re.compile(r"\+-+\+")
SHOWING_N_ROWS_REGEX = re.compile(r".*\(Showing first (\d+) of (\d+) rows\).*")
UNMATERIALIZED_REGEX = re.compile(r".*\(No data to display: Dataframe not materialized\).*")
MATERIALIZED_NO_ROWS_REGEX = re.compile(r".*\(No data to display: Materialized dataframe has no rows\).*")


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
    for line in lines[4:-1]:
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
    assert parse_html_table(df._repr_html_(), expected_user_msg_regex=UNMATERIALIZED_REGEX) == expected_data

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
    assert parse_html_table(df._repr_html_(), expected_user_msg_regex=MATERIALIZED_NO_ROWS_REGEX) == expected_data


def test_alias_repr():
    df = daft.from_pydict({"A": [1, 2, 3], "B": ["a", "b", "c"]})
    df = df.select(df["A"].alias("A2"), df["B"])

    expected_data = {"A2": ("Int64", []), "B": ("Utf8", [])}
    assert parse_str_table(df.__repr__(), expected_user_msg_regex=UNMATERIALIZED_REGEX) == expected_data
    assert parse_html_table(df._repr_html_(), expected_user_msg_regex=UNMATERIALIZED_REGEX) == expected_data

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
    assert parse_html_table(df._repr_html_()) == expected_data_html
