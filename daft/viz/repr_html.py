from __future__ import annotations

import base64
import io
from typing import Any

import pandas as pd
from tabulate import tabulate

from daft.dataframe.schema import DataFrameSchema, DataFrameSchemaField
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


def _stringify_object_field(max_col_width: int = DEFAULT_MAX_COL_WIDTH, max_lines: int = DEFAULT_MAX_LINES):
    def _stringify(val: Any):
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

    return _stringify


def _stringify_any(max_col_width: int = DEFAULT_MAX_COL_WIDTH, max_lines: int = DEFAULT_MAX_LINES):
    def _stringify(val: Any):
        return _truncate(str(val), max_col_width, max_lines)

    return _stringify


def pd_df_repr_html(
    pd_df: pd.DataFrame,
    daft_schema: DataFrameSchema,
    max_col_width: int = DEFAULT_MAX_COL_WIDTH,
    max_lines: int = DEFAULT_MAX_LINES,
) -> str:
    """Converts a Pandas dataframe into a HTML string"""

    def stringify_column(pd_col: pd.Series, field: DataFrameSchemaField) -> pd.Series:
        # Special handling for visualizing if we know the field
        if field is not None:
            if not ExpressionType.is_primitive(field.daft_type):
                return pd_col.map(_stringify_object_field(max_col_width=max_col_width, max_lines=max_lines))
        return pd_col.map(_stringify_any(max_col_width=max_col_width, max_lines=max_lines))

    stringified_df = pd.DataFrame(
        {
            colname: stringify_column(
                pd_df[colname],
                daft_schema[colname],
            )
            for colname in daft_schema.column_names()
        }
    )

    tabulate_html_string = tabulate(
        stringified_df,
        headers=[f"{name}<br>{daft_schema[name].daft_type}" for name in daft_schema.column_names()],
        tablefmt="unsafehtml",
        showindex=False,
        missingval="None",
    )

    # Appending class="dataframe" here helps Google Colab with applying CSS
    assert tabulate_html_string.startswith("<table")
    tabulate_html_string = '<table class="dataframe"' + tabulate_html_string[len("<table") :]

    return f"""
        <div>
            {tabulate_html_string}
            <small>(Showing first {len(pd_df)} rows)</small>
        </div>
    """
