from __future__ import annotations

import daft


def test_hudi_read_table(unzip_table_0_x_cow_partitioned):
    path = unzip_table_0_x_cow_partitioned
    df = daft.read_hudi(path)
    assert df.schema().column_names() == [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "ts",
        "uuid",
        "rider",
        "driver",
        "fare",
        "city",
    ]
    assert df.select("rider").sort("rider").to_pydict() == {
        "rider": ["rider-A", "rider-C", "rider-D", "rider-F", "rider-J"]
    }
