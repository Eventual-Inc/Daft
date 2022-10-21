from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pandas

from daft.dataframe.schema import DataFrameSchema
from daft.viz.repr_html import pd_df_repr_html

HAS_PILLOW = False
try:
    pass

    HAS_PILLOW = True
except ImportError:
    pass
if HAS_PILLOW:
    pass


@dataclass(frozen=True)
class DataFrameDisplay:

    pd_df: pandas.DataFrame
    schema: DataFrameSchema
    column_char_width: int = 20
    max_col_rows: int = 3

    def _repr_html_(self) -> str:
        return pd_df_repr_html(self.pd_df, self.schema)

    def __repr__(self) -> str:
        return cast(str, self.pd_df.__repr__())
