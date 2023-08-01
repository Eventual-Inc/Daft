from __future__ import annotations

import tempfile

import pyarrow as pa
import pyarrow.parquet as papq


def generate_parquet_file():
    """Generate a small Parquet file and return the path to the file"""
    tmpfile = tempfile.NamedTemporaryFile()
    tbl = pa.Table.from_pydict({"foo": [1, 2, 3]})
    papq.write_table(tbl, tmpfile.name)
    return tmpfile
