from __future__ import annotations

from daft.datatype import DataType
from daft.table import Table


def test_schema():
    data = {
        "int": [1, 2, None],
        "float": [1.0, 2.0, None],
        "string": ["a", "b", None],
        "bool": [True, True, None],
    }
    tbl = Table.from_pydict(data)
    schema = tbl.schema()
    assert len(schema) == len(data)
    assert schema.column_names() == list(data.keys())

    expected = {
        "int": DataType.int64(),
        "float": DataType.float64(),
        "string": DataType.string(),
        "bool": DataType.bool(),
    }
    for key in expected:
        assert schema[key].name == key
        assert schema[key].dtype == expected[key]
