from __future__ import annotations

import html
from typing import Any, Callable, Iterable

from tabulate import tabulate

from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.table import Table
from daft.viz.html_viz_hooks import get_viz_hook

DEFAULT_MAX_COL_WIDTH = 20
DEFAULT_MAX_LINES = 3


def _truncate(s: str, max_col_width: int, max_lines: int):
    """Truncates a string and adds an ellipsis if it exceeds (max_col_width * max_lines) number of characters"""
    max_len = max_col_width * max_lines
    if len(s) > max_len:
        s = s[: (max_col_width * max_lines) - 3] + "..."
    return s


def _stringify_object_default(val: Any, max_col_width: int, max_lines: int):
    """Stringifies Python objects for the REPL"""
    return _truncate(str(val), max_col_width, max_lines)


def _stringify_object_html(val: Any, max_col_width: int, max_lines: int):
    """Stringifies Python objects, with custom handling for specific objects that Daft recognizes as media types"""
    viz_hook = get_viz_hook(val)
    if viz_hook is not None:
        return viz_hook(val)
    return html.escape(_truncate(str(val), max_col_width, max_lines))


def _stringify_vpartition(
    data: dict[str, list[Any]],
    daft_schema: Schema,
    custom_stringify_object: Callable = _stringify_object_default,
    max_col_width: int = DEFAULT_MAX_COL_WIDTH,
    max_lines: int = DEFAULT_MAX_LINES,
) -> dict[str, Iterable[str]]:
    """Converts a vPartition into a dictionary of display-friendly stringified values"""
    assert all(
        colname in data for colname in daft_schema.column_names()
    ), f"Data does not contain columns: {set(daft_schema.column_names()) - set(data.keys())}"

    data_stringified: dict[str, Iterable[str]] = {}
    for colname in daft_schema.column_names():
        field = daft_schema[colname]
        if field.dtype._is_python_type():
            data_stringified[colname] = [
                custom_stringify_object(val, max_col_width, max_lines) for val in data[colname]
            ]
        elif field.dtype == DataType.bool():
            # BUG: tabulate library does not handle string literal values "True" and "False" correctly, so we lowercase them.
            data_stringified[colname] = [_truncate(str(val).lower(), max_col_width, max_lines) for val in data[colname]]
        else:
            data_stringified[colname] = [_truncate(str(val), max_col_width, max_lines) for val in data[colname]]

    return data_stringified


def vpartition_repr_html(
    vpartition: Table | None,
    daft_schema: Schema,
    num_rows: int,
    user_message: str,
    max_col_width: int = DEFAULT_MAX_COL_WIDTH,
    max_lines: int = DEFAULT_MAX_LINES,
) -> str:
    """Converts a vPartition into a HTML string"""
    if len(daft_schema) == 0:
        return "<small>(No data to display: Dataframe has no columns)</small>"
    data = (
        {k: v[:num_rows] for k, v in vpartition.to_pydict().items()}
        if vpartition is not None
        else {colname: [] for colname in daft_schema.column_names()}
    )
    data_stringified = _stringify_vpartition(
        data,
        daft_schema,
        custom_stringify_object=_stringify_object_html,
        max_col_width=max_col_width,
        max_lines=max_lines,
    )

    headers = [f"{name}<br>{daft_schema[name].dtype}" for name in daft_schema.column_names()]

    # Workaround for https://github.com/astanin/python-tabulate/issues/224
    # tabulate library doesn't render header if there are no rows;
    # in that case, work around by printing header as single row.
    if vpartition is None or len(vpartition) == 0:
        tabulate_html_string = tabulate(
            [headers],
            tablefmt="unsafehtml",
            missingval="None",
        )

    else:
        tabulate_html_string = tabulate(
            data_stringified,
            headers=headers,
            tablefmt="unsafehtml",
            missingval="None",
        )

    # tabulate generates empty HTML string for empty table.
    if tabulate_html_string == "":
        tabulate_html_string = "<table></table>"

    # Appending class="dataframe" here helps Google Colab with applying CSS
    assert tabulate_html_string.startswith("<table")
    tabulate_html_string = '<table class="dataframe"' + tabulate_html_string[len("<table") :]

    return f"""<div>
    {tabulate_html_string}
    <small>{user_message}</small>
</div>"""


def vpartition_repr(
    vpartition: Table | None,
    daft_schema: Schema,
    num_rows: int,
    user_message: str,
    max_col_width: int = DEFAULT_MAX_COL_WIDTH,
    max_lines: int = DEFAULT_MAX_LINES,
) -> str:
    """Converts a vPartition into a prettified string for display in a REPL"""
    if len(daft_schema) == 0:
        return "(No data to display: Dataframe has no columns)"

    data = (
        {k: v[:num_rows] for k, v in vpartition.to_pydict().items()}
        if vpartition is not None
        else {colname: [] for colname in daft_schema.column_names()}
    )
    data_stringified = _stringify_vpartition(
        data,
        daft_schema,
        custom_stringify_object=_stringify_object_default,
        max_col_width=max_col_width,
        max_lines=max_lines,
    )

    return (
        tabulate(
            data_stringified,
            headers=[f"{name}\n{daft_schema[name].dtype}" for name in daft_schema.column_names()],
            tablefmt="grid",
            missingval="None",
            # Workaround for https://github.com/astanin/python-tabulate/issues/223
            # If table has no rows, specifying maxcolwidths always raises error.
            maxcolwidths=max_col_width if vpartition is not None and len(vpartition) else None,
        )
        + f"\n{user_message}"
    )
