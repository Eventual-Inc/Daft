import base64
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas
from tabulate import tabulate

from daft.dataframe.schema import DataFrameSchema


@dataclass(frozen=True)
class DataFrameDisplay:

    pd_df: pandas.DataFrame
    schema: DataFrameSchema
    column_char_width: int = 20
    max_col_rows: int = 3

    def _repr_html_(self) -> str:

        max_chars_per_cell = self.max_col_rows * self.column_char_width

        # TODO: we should run this only for PyObj columns
        def stringify_and_truncate(val: Any):
            if hasattr(val, "_repr_png_"):
                png_bytes = val._repr_png_()
                base64_img = base64.b64encode(png_bytes)
                return f'<img style="max-height:128px;width:auto" src="data:image/png;base64, {base64_img.decode("utf-8")}" alt="{str(val)}" />'
            elif isinstance(val, np.ndarray):
                data_str = np.array2string(val, threshold=3)
                data_str = (
                    data_str if len(data_str) <= max_chars_per_cell else data_str[: max_chars_per_cell - 5] + "...]"
                )
                return f"&ltnp.ndarray<br>&nbspshape={val.shape}<br>&nbspdtype={val.dtype}<br>&nbspdata={data_str}&gt"
            else:
                s = str(val)
            return s if len(s) <= max_chars_per_cell else s[: max_chars_per_cell - 4] + "..."

        pd_df = self.pd_df.applymap(stringify_and_truncate)
        table = tabulate(
            pd_df,
            headers=[f"{name}<br>{self.schema[name].daft_type}" for name in self.schema.column_names()],
            tablefmt="unsafehtml",
            showindex=False,
            missingval="None",
        )
        table_string = table._repr_html_().replace("<td>", '<td style="text-align: left;">')  # type: ignore
        return f"""
            <div>
                {table_string}
                <p>(Showing first {len(pd_df)} rows)</p>
            </div>
        """

    def __repr__(self) -> str:
        max_chars_per_cell = self.max_col_rows * self.column_char_width

        def stringify_and_truncate(val: Any):
            s = str(val)
            return s if len(s) <= max_chars_per_cell else s[: max_chars_per_cell - 4] + "..."

        pd_df = self.pd_df.applymap(stringify_and_truncate)
        return tabulate(
            pd_df,
            headers=[f"{name}\n{self.schema[name].daft_type}" for name in self.schema.column_names()],
            showindex=False,
            missingval="None",
            maxcolwidths=20,
        )
