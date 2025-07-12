from __future__ import annotations

import re
from typing import Any

import pyarrow as pa

from daft.recordbatch import RecordBatch

ANSI_ESCAPE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

TD_STYLE = 'style="text-align:left; max-width:192px; max-height:64px; overflow:auto"'
TH_STYLE = 'style="text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left"'


def sort_arrow_table(tbl: pa.Table, *sort_by: str, ascending: bool = False):
    return tbl.sort_by([(name, "ascending" if ascending else "descending") for name in sort_by])


def assert_pyarrow_tables_equal(from_daft: pa.Table, expected: pa.Table):
    # Do a round-trip with Daft in order to cast pyarrow dtypes to Daft's supported Arrow dtypes (e.g. string -> large_string).
    expected = RecordBatch.from_arrow_table(expected).to_arrow_table()
    assert from_daft == expected, f"from_daft = {from_daft}\n\nexpected = {expected}"


def sort_pydict(pydict: dict[str, list[Any]], *sort_by: str, ascending: bool = False):
    return sort_arrow_table(RecordBatch.from_pydict(pydict).to_arrow_table(), *sort_by, ascending=ascending).to_pydict()
