from __future__ import annotations

from dataclasses import dataclass

from daft.dataframe.schema import DataFrameSchema
from daft.runners.partitioning import vPartition
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

    vpartition: vPartition
    schema: DataFrameSchema
    column_char_width: int = 20
    max_col_rows: int = 3

    def _repr_html_(self) -> str:
        return vpartition_repr_html(self.vpartition, self.schema)

    def __repr__(self) -> str:
        return vpartition_repr(self.vpartition, self.schema)
