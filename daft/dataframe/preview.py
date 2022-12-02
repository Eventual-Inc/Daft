from __future__ import annotations

from dataclasses import dataclass

from daft.runners.partitioning import vPartition


@dataclass(frozen=True)
class DataFramePreview:
    """A class containing all the metadata/data required to preview a dataframe."""

    preview_partition: vPartition | None
    dataframe_num_rows: int | None
