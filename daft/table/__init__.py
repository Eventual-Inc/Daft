from __future__ import annotations

import os

from .table import Table, read_parquet_into_pyarrow, read_parquet_into_pyarrow_bulk

# Need to import after `.table` due to circular dep issue otherwise
from .micropartition import MicroPartition as _MicroPartition  # isort:skip


# Use $DAFT_MICROPARTITIONS envvar as a feature flag to turn on MicroPartitions
LegacyTable = Table
if os.getenv("DAFT_MICROPARTITIONS", "0") == "1":
    Table = _MicroPartition  # type: ignore


__all__ = ["Table", "LegacyTable", "read_parquet_into_pyarrow", "read_parquet_into_pyarrow_bulk"]
