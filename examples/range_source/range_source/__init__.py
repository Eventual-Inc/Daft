from __future__ import annotations

from typing import TYPE_CHECKING

import daft

if TYPE_CHECKING:
    from daft import DataFrame


def range(n: int = 1000, partitions: int = 4) -> DataFrame:
    """Read a range of integers as a DataFrame.

    Args:
        n: Number of values to generate.
        partitions: Number of partitions (tasks).
    """
    return daft.read_source("range", n=n, partitions=partitions)
