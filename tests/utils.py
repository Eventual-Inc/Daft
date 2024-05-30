from __future__ import annotations

import re

import pyarrow as pa
import pyarrow.compute as pac

from daft.table import Table

ANSI_ESCAPE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

TD_STYLE = 'style="text-align:left; max-width:192px; max-height:64px; overflow:auto"'
TH_STYLE = 'style="text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left"'


def sort_arrow_table(tbl: pa.Table, sort_by: str):
    """In arrow versions < 7, pa.Table does not support sorting yet so we add a helper method here"""
    sort_indices = pac.sort_indices(tbl.column(sort_by))
    return pac.take(tbl, sort_indices)


def assert_pyarrow_tables_equal(from_daft: pa.Table, expected: pa.Table):
    # Do a round-trip with Daft in order to cast pyarrow dtypes to Daft's supported Arrow dtypes (e.g. string -> large_string).
    expected = Table.from_arrow(expected).to_arrow()
    assert from_daft == expected, f"from_daft = {from_daft}\n\nexpected = {expected}"
