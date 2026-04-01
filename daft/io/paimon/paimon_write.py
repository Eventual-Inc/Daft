"""Paimon write utilities for Daft.

This module provides utility functions for writing to Apache Paimon tables,
including schema conversion and patches for pypaimon compatibility.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def _patch_pypaimon_stats_for_complex_types() -> None:
    """Patch pypaimon's _get_column_stats to handle complex types.

    pypaimon computes column statistics (min/max) for all columns when writing.
    However, PyArrow's min/max functions don't support list, map, or struct types,
    which causes ArrowNotImplementedError.

    This patch makes the stats computation skip complex types, returning None
    for their min/max values instead of failing.

    This is a workaround for: https://github.com/apache/paimon-python/issues
    """
    from pypaimon.write.writer.data_writer import DataWriter

    def _patched_get_column_stats(record_batch: pa.RecordBatch, column_name: str) -> dict:
        """Patched version that handles complex types gracefully."""
        column_array = record_batch.column(column_name)
        if column_array.null_count == len(column_array):
            return {
                "min_values": None,
                "max_values": None,
                "null_counts": column_array.null_count,
            }

        dtype = column_array.type
        # Skip min/max computation for complex types that PyArrow doesn't support
        if pa.types.is_list(dtype) or pa.types.is_map(dtype) or pa.types.is_struct(dtype):
            return {
                "min_values": None,
                "max_values": None,
                "null_counts": column_array.null_count,
            }

        min_values = pc.min(column_array).as_py()
        max_values = pc.max(column_array).as_py()
        null_counts = column_array.null_count
        return {
            "min_values": min_values,
            "max_values": max_values,
            "null_counts": null_counts,
        }

    # Apply the patch
    DataWriter._get_column_stats = staticmethod(_patched_get_column_stats)


# Apply the patch at module load time
_patch_pypaimon_stats_for_complex_types()
