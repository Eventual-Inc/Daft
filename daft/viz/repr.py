from __future__ import annotations

import base64
import io
from typing import Any, Callable, Iterable, Sequence

from tabulate import tabulate

from daft.dataframe.schema import DataFrameSchema
from daft.runners.partitioning import vPartition
from daft.types import ExpressionType

try:
    import PIL.Image

    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False

try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


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
    if HAS_PILLOW and isinstance(val, PIL.Image.Image):
        img = val.copy()
        img.thumbnail((128, 128))
        bio = io.BytesIO()
        img.save(bio, "JPEG")
        base64_img = base64.b64encode(bio.getvalue())
        return f'<img style="max-height:128px;width:auto" src="data:image/png;base64, {base64_img.decode("utf-8")}" alt="{str(val)}" />'
    elif HAS_NUMPY and isinstance(val, np.ndarray):
        return f"&ltnp.ndarray<br>shape={val.shape}<br>dtype={val.dtype}&gt"
    return _truncate(str(val), max_col_width, max_lines)


def _stringify_vpartition(
    data: dict[str, Sequence[Any]],
    daft_schema: DataFrameSchema,
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
        if ExpressionType.is_py(field.daft_type):
            data_stringified[colname] = [
                custom_stringify_object(val, max_col_width, max_lines) for val in data[colname]
            ]
        else:
            data_stringified[colname] = [_truncate(str(val), max_col_width, max_lines) for val in data[colname]]

    return data_stringified


def vpartition_repr_html(
    vpartition: vPartition | None,
    daft_schema: DataFrameSchema,
    num_rows: int,
    user_message: str,
    max_col_width: int = DEFAULT_MAX_COL_WIDTH,
    max_lines: int = DEFAULT_MAX_LINES,
) -> str:
    """Converts a vPartition into a HTML string"""
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

    headers = [f"{name}<br>{daft_schema[name].daft_type}" for name in daft_schema.column_names()]

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

    return f"""
        <div>
            {tabulate_html_string}
            <small>{user_message}</small>
        </div>
    """


def vpartition_repr(
    vpartition: vPartition | None,
    daft_schema: DataFrameSchema,
    num_rows: int,
    user_message: str,
    max_col_width: int = DEFAULT_MAX_COL_WIDTH,
    max_lines: int = DEFAULT_MAX_LINES,
) -> str:
    """Converts a vPartition into a prettified string for display in a REPL"""
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
            headers=[f"{name}\n{daft_schema[name].daft_type}" for name in daft_schema.column_names()],
            tablefmt="grid",
            missingval="None",
            # Workaround for https://github.com/astanin/python-tabulate/issues/223
            # If table has no rows, specifying maxcolwidths always raises error.
            maxcolwidths=max_col_width if vpartition is not None and len(vpartition) else None,
        )
        + f"\n{user_message}"
    )
