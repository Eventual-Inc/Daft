from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pac


def sort_arrow_table(tbl: pa.Table, sort_by: str):
    """In arrow versions < 7, pa.Table does not support sorting yet so we add a helper method here"""
    sort_indices = pac.sort_indices(tbl.column(sort_by))
    return pac.take(tbl, sort_indices)
