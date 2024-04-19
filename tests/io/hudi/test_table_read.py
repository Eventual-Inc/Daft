from __future__ import annotations

import datetime

import daft


def test_hudi_read_table(test_table_path):
    df = daft.read_hudi(test_table_path)
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
