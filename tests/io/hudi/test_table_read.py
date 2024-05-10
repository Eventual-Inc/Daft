from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

import daft

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LE_8_0_0, reason="hudi only supported if pyarrow >= 8.0.0")


def test_read_table(get_testing_table_for_supported_cases):
    table_path = get_testing_table_for_supported_cases
    df = daft.read_hudi(table_path)

    # when it's case `v6_simplekeygen_hivestyle_no_metafields`
    # hoodie.populate.meta.fields=false, meta fields are still present in schema
    assert df.schema().column_names()[:8] == [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "id",
        "name",
        "isActive",
    ]
    assert df.select("name", "isActive", "dateField", "structField").sort("name").to_pydict() == {
        "name": ["Alice", "Bob", "Carol", "Diana"],
        "isActive": [False, False, True, True],
        "dateField": [
            datetime.date(2023, 4, 1),
            datetime.date(2023, 4, 2),
            datetime.date(2023, 4, 3),
            datetime.date(2023, 4, 4),
        ],
        "structField": [
            {
                "field1": "Alice",
                "field2": 30,
                "child_struct": {"child_field1": 123.456, "child_field2": True},
            },
            {
                "field1": "Bob",
                "field2": 40,
                "child_struct": {"child_field1": 789.012, "child_field2": False},
            },
            {
                "field1": "Carol",
                "field2": 25,
                "child_struct": {"child_field1": 456.789, "child_field2": True},
            },
            {
                "field1": "Diana",
                "field2": 50,
                "child_struct": {"child_field1": 987.654, "child_field2": True},
            },
        ],
    }


def test_read_empty_table(get_empty_table):
    table_path = get_empty_table
    df = daft.read_hudi(table_path)
    assert len(df.collect()) == 0
