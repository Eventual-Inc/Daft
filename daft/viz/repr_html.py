import base64
import io
from typing import Any

import numpy as np
import pandas as pd
from tabulate import tabulate

from daft.dataframe.schema import DataFrameSchema, DataFrameSchemaField
from daft.execution.operators import ExpressionType

try:
    import PIL.Image

    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False


def _stringify_object_field(val: Any):
    if HAS_PILLOW and isinstance(val, PIL.Image.Image):
        img = val.copy()
        img.thumbnail((128, 128))
        bio = io.BytesIO()
        img.save(bio, "JPEG")
        base64_img = base64.b64encode(bio.getvalue())
        return f'<img style="max-height:128px;width:auto" src="data:image/png;base64, {base64_img.decode("utf-8")}" alt="{str(val)}" />'
    elif isinstance(val, np.ndarray):
        return f"&ltnp.ndarray<br>&nbspshape={val.shape}<br>&nbspdtype={val.dtype}&gt"
    return str(val)


def pd_df_repr_html(pd_df: pd.DataFrame, daft_schema: DataFrameSchema) -> str:
    """Converts a Pandas dataframe into a HTML string"""

    def stringify_column(pd_col: pd.Series, field: DataFrameSchemaField) -> pd.Series:
        # Special handling for visualizing if we know the field
        if field is not None:
            if not ExpressionType.is_primitive(field.daft_type):
                return pd_col.map(_stringify_object_field)
        return pd_col.map(str)

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
    assert tabulate_html_string.startswith("<table")
    tabulate_html_string = '<table class="dataframe"' + tabulate_html_string[len("<table") :]
    return tabulate_html_string
