from __future__ import annotations

import re

import pyarrow as pa
import pyarrow.compute as pac

ANSI_ESCAPE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


def sort_arrow_table(tbl: pa.Table, sort_by: str):
    """In arrow versions < 7, pa.Table does not support sorting yet so we add a helper method here"""
    sort_indices = pac.sort_indices(tbl.column(sort_by))
    return pac.take(tbl, sort_indices)
