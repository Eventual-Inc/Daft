from __future__ import annotations

import os

from .table import Table, read_parquet_into_pyarrow, read_parquet_into_pyarrow_bulk

# Need to import after `.table` due to circular dep issue otherwise
from .micropartition import MicroPartition as _MicroPartition  # isort:skip


MicroPartition = _MicroPartition

# Use $DAFT_MICROPARTITIONS envvar as a feature flag to turn off MicroPartitions
if os.getenv("DAFT_MICROPARTITIONS", "1") != "1":
    MicroPartition = Table  # type: ignore


__all__ = ["MicroPartition", "Table", "read_parquet_into_pyarrow", "read_parquet_into_pyarrow_bulk"]
