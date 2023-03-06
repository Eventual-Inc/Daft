from __future__ import annotations

from dataclasses import dataclass

from daft.table import Table


@dataclass(frozen=True)
class DataFramePreview:
    """A class containing all the metadata/data required to preview a dataframe."""

    preview_partition: Table | None
    dataframe_num_rows: int | None
