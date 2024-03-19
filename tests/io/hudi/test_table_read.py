from __future__ import annotations

import daft
from daft.hudi.pyhudi.table import HudiTable
from daft.logical.schema import Schema


def test_hudi_read_table():
    path = "/tmp/trips_table"
    df = daft.read_hudi(path)
    table_schema = HudiTable(path).schema
    expected_schema = Schema.from_pyarrow_schema(table_schema)
    assert df.schema() == expected_schema
    df.show()
