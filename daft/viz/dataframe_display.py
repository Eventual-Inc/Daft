from __future__ import annotations

from dataclasses import dataclass

from daft.dataframe.preview import DataFramePreview
from daft.dataframe.schema import DataFrameSchema
from daft.viz.repr import vpartition_repr, vpartition_repr_html

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

    preview: DataFramePreview
    schema: DataFrameSchema
    column_char_width: int = 20
    max_col_rows: int = 3
    num_rows: int = 10

    def _get_user_message(self) -> str:
        if self.preview.preview_partition is None:
            return "(No data to display: Dataframe not materialized)"
        if self.preview.dataframe_num_rows == 0:
            return "(Materialized dataframe has no rows)"
        if self.preview.dataframe_num_rows is None:
            return f"(Showing first {min(self.num_rows, len(self.preview.preview_partition))} rows)"
        return f"(Showing first {min(self.num_rows, len(self.preview.preview_partition))} of {self.preview.dataframe_num_rows} rows)"

    def _repr_html_(self) -> str:
        return vpartition_repr_html(
            self.preview.preview_partition,
            self.schema,
            self.num_rows,
            self._get_user_message(),
            max_col_width=self.column_char_width,
            max_lines=self.max_col_rows,
        )

    def __repr__(self) -> str:
        return vpartition_repr(
            self.preview.preview_partition,
            self.schema,
            self.num_rows,
            self._get_user_message(),
            max_col_width=self.column_char_width,
            max_lines=self.max_col_rows,
        )
