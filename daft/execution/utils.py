from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.runners.partitioning import MaterializedResult, PartitionT


def get_in_memory_partitions(
    psets: dict[str, list[MaterializedResult[PartitionT]]], cache_key: str
) -> list | None:
    """Get micropartitions from psets by cache key.
    
    Args:
        psets: Dictionary mapping cache keys to lists of MaterializedResult objects
        cache_key: The cache key to look up in psets
        
    Returns:
        List of PyMicroPartition objects or None if cache_key not found
    """
    if parts_list := psets.get(cache_key):
        # Extract micropartitions from MaterializedResult objects
        return [part.micropartition()._micropartition for part in parts_list]
    return None
